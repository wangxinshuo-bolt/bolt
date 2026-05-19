/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/ProcessSpillService.h"

#include <algorithm>
#include <csignal>
#include <filesystem>
#include <mutex>
#include <unistd.h>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/DiskProbe.h"

namespace bytedance::bolt::memory::bm {
namespace {

struct SingletonState {
  std::mutex mutex;
  bool configured{false};
  ProcessSpillServiceConfig pendingConfig;
  std::unique_ptr<ProcessSpillService> instance;
};

SingletonState& globalState() {
  static SingletonState state;
  return state;
}

uint32_t resolveWorkerThreadCount(uint32_t configured) {
  // Pass through unchanged: 0 means caller wants synchronous fallback.
  return configured;
}

MetricsRegistry& effectiveRegistry(MetricsRegistry* metrics) {
  return metrics == nullptr ? NoOpMetricsRegistry() : *metrics;
}

} // namespace

ProcessSpillService::ProcessSpillService(ProcessSpillServiceConfig config)
    : config_(std::move(config)),
      metrics_(effectiveRegistry(config_.metrics)),
      scheduler_(std::make_unique<GlobalSpillScheduler>(
          resolveWorkerThreadCount(config_.workerThreadCount),
          metrics_)) {
  BOLT_USER_CHECK(
      !config_.dirs.empty(),
      "ProcessSpillService requires at least one spill directory");
  CleanupStaleDirsAtStartup();
  bool configuredDiskIo = false;
  for (const auto& dir : config_.dirs) {
    SpillStoreConfig storeCfg;
    storeCfg.spillDir = dir.path;
    storeCfg.cleanupOnDestroy = config_.cleanupOnDestroy;
    storeCfg.forcedKind = dir.forcedKind;
    storeCfg.unknownFallbackKind = config_.unknownFallbackKind;
    DiskProbeConfig probeConfig;
    probeConfig.directory = storeCfg.spillDir;
    probeConfig.duration = config_.diskProbeDuration;
    probeConfig.forcedKind = storeCfg.forcedKind;
    probeConfig.fallbackKind = storeCfg.unknownFallbackKind;
    storeCfg.diskProbe = ProbeDisk(probeConfig);
    if (!configuredDiskIo) {
      DiskIoConfig ioConfig;
      ioConfig.backend = DiskIoBackend::kUring;
      ioConfig.targetP95LatencyUs = storeCfg.diskProbe.targetP95LatencyUs;
      ProcessDiskIoService::ConfigureDefaultIfNeeded(ioConfig);
      configuredDiskIo = true;
    }
    SpillStore::CleanupAtStartup(storeCfg);
    stores_.push_back(
        std::make_unique<SpillStore>(storeCfg, config_.metrics));
  }
  scheduler_->Start();
  BOLT_MEM_LOG(INFO) << "ProcessSpillService initialized with "
                     << stores_.size() << " stores";
}

ProcessSpillService::~ProcessSpillService() {
  scheduler_->Stop();
  scheduler_.reset();
  stores_.clear();
}

ProcessSpillService& ProcessSpillService::Instance() {
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  if (state.instance != nullptr) {
    return *state.instance;
  }
  BOLT_USER_CHECK(
      state.configured,
      "ProcessSpillService::Instance() called without ConfigureDefault");
  state.instance.reset(
      new ProcessSpillService(std::move(state.pendingConfig)));
  return *state.instance;
}

void ProcessSpillService::ConfigureDefault(ProcessSpillServiceConfig config) {
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  BOLT_USER_CHECK(
      !state.configured && state.instance == nullptr,
      "ProcessSpillService::ConfigureDefault may be called only once");
  BOLT_USER_CHECK(
      !config.dirs.empty(),
      "ProcessSpillServiceConfig.dirs must not be empty");
  state.pendingConfig = std::move(config);
  state.configured = true;
}

void ProcessSpillService::ResetForTesting() {
  auto& state = globalState();
  std::unique_ptr<ProcessSpillService> dying;
  {
    std::lock_guard<std::mutex> l(state.mutex);
    if (state.instance != nullptr) {
      std::lock_guard<std::mutex> cl(state.instance->clientsMutex_);
      for (const auto& [id, weak] : state.instance->clients_) {
        BOLT_USER_CHECK(
            weak.expired(),
            "ResetForTesting called while SpillClient {} is still live",
            id);
      }
    }
    dying = std::move(state.instance);
    state.configured = false;
    state.pendingConfig = ProcessSpillServiceConfig{};
  }
  // Destroy outside the singleton mutex so worker threads can join cleanly.
  dying.reset();
}

std::unique_ptr<ProcessSpillService> ProcessSpillService::CreateForTesting(
    ProcessSpillServiceConfig config) {
  return std::unique_ptr<ProcessSpillService>(
      new ProcessSpillService(std::move(config)));
}

std::shared_ptr<SpillClient> ProcessSpillService::CreateClient(
    SpillClientConfig config) {
  const auto schedulerId =
      scheduler_->RegisterClient(config.fairnessWeight);
  std::shared_ptr<SpillClient> client(
      new SpillClient(this, schedulerId, std::move(config)));
  std::lock_guard<std::mutex> l(clientsMutex_);
  clients_[schedulerId] = client;
  return client;
}

SpillStore& ProcessSpillService::PickStore() {
  // Round-robin among configured stores. The set is fixed at init so we
  // never need a lock here.
  return PickStoreForWrite().second;
}

std::pair<uint64_t, SpillStore&> ProcessSpillService::PickStoreForWrite() {
  const auto idx =
      storeRobinCursor_.fetch_add(1, std::memory_order_relaxed) %
      stores_.size();
  return {idx, *stores_[idx]};
}

SpillStore& ProcessSpillService::StoreFor(const SpillLocation& location) {
  BOLT_USER_CHECK(location.Valid(), "Invalid spill location");
  BOLT_USER_CHECK_LT(
      location.storeIndex,
      stores_.size(),
      "Invalid spill store index {} for {} stores",
      location.storeIndex,
      stores_.size());
  return *stores_[location.storeIndex];
}

void ProcessSpillService::ChargeQuota(SpillClient& client, ByteCount bytes) {
  if (config_.processDiskQuotaBytes != 0) {
    auto current = usedDiskBytes_.load(std::memory_order_relaxed);
    while (true) {
      BOLT_USER_CHECK(
          current <= config_.processDiskQuotaBytes &&
              bytes <= config_.processDiskQuotaBytes - current,
          "ProcessSpillService disk quota exceeded: used={} request={} limit={}",
          current,
          bytes,
          config_.processDiskQuotaBytes);
      if (usedDiskBytes_.compare_exchange_weak(
              current,
              current + bytes,
              std::memory_order_relaxed)) {
        break;
      }
    }
  } else {
    usedDiskBytes_.fetch_add(bytes, std::memory_order_relaxed);
  }

  if (client.config_.diskQuotaBytes != 0) {
    auto current = client.usedDiskBytes_.load(std::memory_order_relaxed);
    while (true) {
      if (current > client.config_.diskQuotaBytes ||
          bytes > client.config_.diskQuotaBytes - current) {
        // Roll back the process-level reservation we just took.
        usedDiskBytes_.fetch_sub(bytes, std::memory_order_relaxed);
        BOLT_USER_FAIL(
            "SpillClient {} disk quota exceeded: used={} request={} limit={}",
            client.config_.tenantId,
            current,
            bytes,
            client.config_.diskQuotaBytes);
      }
      if (client.usedDiskBytes_.compare_exchange_weak(
              current,
              current + bytes,
              std::memory_order_relaxed)) {
        break;
      }
    }
  } else {
    client.usedDiskBytes_.fetch_add(bytes, std::memory_order_relaxed);
  }
}

void ProcessSpillService::CreditQuota(
    SpillClient& client,
    ByteCount bytes) noexcept {
  if (bytes == 0) {
    return;
  }
  auto current = usedDiskBytes_.load(std::memory_order_relaxed);
  const auto sub = std::min(current, bytes);
  if (sub != 0) {
    usedDiskBytes_.fetch_sub(sub, std::memory_order_relaxed);
  }
  auto clientCurrent = client.usedDiskBytes_.load(std::memory_order_relaxed);
  const auto clientSub = std::min(clientCurrent, bytes);
  if (clientSub != 0) {
    client.usedDiskBytes_.fetch_sub(clientSub, std::memory_order_relaxed);
  }
}

void ProcessSpillService::UnregisterClient(uint64_t clientId) {
  std::lock_guard<std::mutex> l(clientsMutex_);
  clients_.erase(clientId);
}

void ProcessSpillService::CleanupStaleDirsAtStartup() {
  // For each configured spill dir, scan its parent for sibling
  // bolt_spill_<pid>_* directories whose pid is no longer alive and remove
  // them. Best-effort and never throws.
  for (const auto& dir : config_.dirs) {
    std::error_code ec;
    std::filesystem::path path(dir.path);
    if (path.empty()) {
      continue;
    }
    auto parent = path.parent_path();
    if (parent.empty() || !std::filesystem::exists(parent, ec)) {
      continue;
    }
    for (const auto& entry :
         std::filesystem::directory_iterator(parent, ec)) {
      if (ec) {
        break;
      }
      if (!entry.is_directory(ec)) {
        continue;
      }
      const auto name = entry.path().filename().string();
      constexpr std::string_view kPrefix{"bolt_spill_"};
      if (name.rfind(kPrefix, 0) != 0) {
        continue;
      }
      const auto rest = name.substr(kPrefix.size());
      const auto sep = rest.find('_');
      if (sep == std::string::npos) {
        continue;
      }
      pid_t pid = 0;
      try {
        pid = static_cast<pid_t>(std::stoi(rest.substr(0, sep)));
      } catch (...) {
        continue;
      }
      if (pid == ::getpid()) {
        continue;
      }
      // kill(pid, 0) returns 0 if alive, -1/ESRCH if not.
      if (::kill(pid, 0) == 0) {
        continue;
      }
      std::error_code rmEc;
      std::filesystem::remove_all(entry.path(), rmEc);
      if (rmEc) {
        BOLT_MEM_LOG(WARNING)
            << "ProcessSpillService failed to clean stale dir "
            << entry.path().string() << ": " << rmEc.message();
      } else {
        BOLT_MEM_LOG(INFO)
            << "ProcessSpillService cleaned stale dir "
            << entry.path().string();
      }
    }
  }
}

} // namespace bytedance::bolt::memory::bm
