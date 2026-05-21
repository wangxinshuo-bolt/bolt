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
#include "bolt/common/memory/bm/BlockHandle.h"
#include "bolt/common/memory/bm/DiskProbe.h"
#include "bolt/common/memory/bm/Observability.h"

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

} // namespace

ProcessSpillService::ProcessSpillService(ProcessSpillServiceConfig config)
    : config_(std::move(config)),
      metrics_(EffectiveMetricsRegistry(config_.metrics)),
      submitCounter_(metrics_.GetCounter("bm_spill_submit_total", "")),
      scheduledCounter_(metrics_.GetCounter("bm_spill_scheduled_total", "")),
      backpressuredCounter_(
          metrics_.GetCounter("bm_spill_backpressured_total", "")),
      skippedCounter_(metrics_.GetCounter("bm_spill_skipped_total", "")),
      failedCounter_(metrics_.GetCounter("bm_spill_failed_total", "")),
      executedCounter_(metrics_.GetCounter("bm_spill_executed_total", "")),
      freedBytesCounter_(metrics_.GetCounter("bm_spill_freed_bytes_total", "")),
      queueDepthGauge_(metrics_.GetGauge("bm_spill_queue_depth", "")),
      submitDuration_(metrics_.GetHistogram("bm_spill_submit_duration_us", "")),
      executeDuration_(
          metrics_.GetHistogram("bm_spill_execute_duration_us", "")),
      waitForProgressDuration_(
          metrics_.GetHistogram("bm_wait_for_spill_progress_duration_us", "")) {
  BOLT_USER_CHECK(
      !config_.spillDir.empty(),
      "ProcessSpillService requires a spill directory");
  CleanupStaleDirsAtStartup();
  SpillStoreConfig storeCfg;
  storeCfg.spillDir = config_.spillDir;
  storeCfg.cleanupOnDestroy = config_.cleanupOnDestroy;
  storeCfg.forcedKind = config_.forcedKind;
  storeCfg.unknownFallbackKind = config_.unknownFallbackKind;
  storeCfg.smallSpill = config_.smallSpill;
  storeCfg.compression = config_.compression;
  DiskProbeConfig probeConfig = config_.diskProbe;
  probeConfig.directory = storeCfg.spillDir;
  probeConfig.duration = config_.diskProbeDuration;
  probeConfig.forcedKind = storeCfg.forcedKind;
  probeConfig.fallbackKind = storeCfg.unknownFallbackKind;
  storeCfg.diskProbe = ProbeDisk(probeConfig);
  ProcessDiskIoService::ConfigureDefaultIfNeeded(config_.diskIo);
  SpillStore::CleanupAtStartup(storeCfg);
  store_ = std::make_unique<SpillStore>(storeCfg, config_.metrics);
  StartWorkers();
  BOLT_MEM_LOG(INFO) << "ProcessSpillService initialized"
                     << " spillDir=" << config_.spillDir
                     << " workerThreadCount=" << config_.workerThreadCount
                     << " cleanupOnDestroy=" << config_.cleanupOnDestroy
                     << " diskKind=" << ToString(storeCfg.diskProbe.kind)
                     << " probeActive=" << storeCfg.diskProbe.activeProbeRan
                     << " writeIops=" << storeCfg.diskProbe.writeIops
                     << " readIops=" << storeCfg.diskProbe.readIops;
}

ProcessSpillService::~ProcessSpillService() {
  StopWorkers();
  store_.reset();
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
      !config.spillDir.empty(),
      "ProcessSpillServiceConfig.spillDir must not be empty");
  state.pendingConfig = std::move(config);
  state.configured = true;
}

void ProcessSpillService::ResetForTesting() {
  auto& state = globalState();
  std::unique_ptr<ProcessSpillService> dying;
  {
    std::lock_guard<std::mutex> l(state.mutex);
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

EvictResult ProcessSpillService::SubmitSpill(EvictionNode node) {
  ScopedBmTimer timer(submitDuration_);
  submitCounter_.Add(1);
  if (node.block.expired()) {
    skippedCounter_.Add(1);
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  if (resolveWorkerThreadCount(config_.workerThreadCount) == 0) {
    backpressuredCounter_.Add(1);
    BOLT_MEM_VLOG(1) << "ProcessSpillService backpressured spill submit"
                       << " workerThreadCount=0";
    return EvictResult{EvictResultKind::kBackpressured, 0};
  }
  size_t queueDepth = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopping_) {
      failedCounter_.Add(1);
      return EvictResult{EvictResultKind::kFailed, 0};
    }
    ready_.push_back(std::move(node));
    queueDepth = ready_.size();
    queueDepthGauge_.Set(static_cast<int64_t>(queueDepth));
  }
  scheduledCounter_.Add(1);
  BOLT_MEM_VLOG(1) << "ProcessSpillService scheduled spill submit"
                     << " queueDepth=" << queueDepth;
  cv_.notify_one();
  return EvictResult{EvictResultKind::kScheduled, 0};
}

bool ProcessSpillService::WaitForProgress(
    ByteCount /*bytesNeeded*/,
    std::chrono::milliseconds timeout) {
  ScopedBmTimer timer(waitForProgressDuration_);
  std::unique_lock<std::mutex> l(mutex_);
  const auto startEpoch = progressEpoch_;
  return progressCv_.wait_for(
      l, timeout, [&] { return stopping_ || progressEpoch_ != startEpoch; });
}

SpillLocation ProcessSpillService::Write(
    MemoryTag tag,
    ConstDataPtr src,
    ByteCount bytes) {
  auto location = store_->Write(tag, src, bytes);
  usedDiskBytes_.fetch_add(
      location.slotBytes == 0 ? location.storedBytes : location.slotBytes,
      std::memory_order_relaxed);
  return location;
}

void ProcessSpillService::Read(
    const SpillLocation& location,
    DataPtr dst,
    ByteCount dstCapacity) {
  store_->Read(location, dst, dstCapacity);
}

void ProcessSpillService::Release(const SpillLocation& location) noexcept {
  if (!location.Valid()) {
    return;
  }
  try {
    store_->Release(location);
  } catch (...) {
  }
  auto current = usedDiskBytes_.load(std::memory_order_relaxed);
  const auto bytes =
      location.slotBytes == 0 ? location.storedBytes : location.slotBytes;
  const auto sub = std::min(current, bytes);
  if (sub != 0) {
    usedDiskBytes_.fetch_sub(sub, std::memory_order_relaxed);
  }
}

void ProcessSpillService::StartWorkers() {
  std::lock_guard<std::mutex> l(mutex_);
  if (started_) {
    return;
  }
  started_ = true;
  const auto workerCount = resolveWorkerThreadCount(config_.workerThreadCount);
  for (uint32_t i = 0; i < workerCount; ++i) {
    workers_.emplace_back([this] { WorkerLoop(); });
  }
  queueDepthGauge_.Set(0);
  if (workerCount != 0) {
    BOLT_MEM_LOG(INFO) << "ProcessSpillService started " << workerCount
                       << " spill workers";
  }
}

void ProcessSpillService::StopWorkers() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopping_) {
      return;
    }
    stopping_ = true;
    for (auto& node : ready_) {
      if (auto base = node.block.lock()) {
        if (auto handle = std::dynamic_pointer_cast<BlockHandle>(base)) {
          handle->ClearSpillScheduled();
        }
      }
    }
    ready_.clear();
    queueDepthGauge_.Set(0);
  }
  cv_.notify_all();
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
  NotifyProgress();
}

void ProcessSpillService::WorkerLoop() {
  for (;;) {
    EvictionNode node;
    {
      std::unique_lock<std::mutex> l(mutex_);
      cv_.wait(l, [&] { return stopping_ || !ready_.empty(); });
      if (stopping_) {
        return;
      }
      node = std::move(ready_.front());
      ready_.pop_front();
      queueDepthGauge_.Set(static_cast<int64_t>(ready_.size()));
    }
    ExecuteSpill(std::move(node));
    NotifyProgress();
  }
}

void ProcessSpillService::ExecuteSpill(EvictionNode node) {
  ScopedBmTimer timer(executeDuration_);
  auto base = node.block.lock();
  if (base == nullptr) {
    skippedCounter_.Add(1);
    return;
  }
  auto handle = std::dynamic_pointer_cast<BlockHandle>(base);
  if (handle == nullptr) {
    skippedCounter_.Add(1);
    return;
  }
  if (handle->EvictionSequence() != node.evictionSequence ||
      handle->IsPinned()) {
    handle->ClearSpillScheduled();
    skippedCounter_.Add(1);
    return;
  }
  try {
    const auto bytes = handle->SpillToDisk();
    executedCounter_.Add(1);
    freedBytesCounter_.Add(bytes);
    BOLT_MEM_VLOG(1) << "ProcessSpillService processed block "
                       << handle->Id() << " freed=" << bytes;
  } catch (const std::exception& e) {
    handle->ClearSpillScheduled();
    failedCounter_.Add(1);
    BOLT_MEM_LOG(WARNING)
        << "ProcessSpillService failed to spill block " << handle->Id()
        << ": " << e.what();
  }
}

void ProcessSpillService::NotifyProgress() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++progressEpoch_;
  }
  progressCv_.notify_all();
}

void ProcessSpillService::CleanupStaleDirsAtStartup() {
  // For each configured spill dir, scan its parent for sibling
  // bolt_spill_<pid>_* directories whose pid is no longer alive and remove
  // them. Best-effort and never throws.
  {
    std::error_code ec;
    std::filesystem::path path(config_.spillDir);
    if (path.empty()) {
      return;
    }
    auto parent = path.parent_path();
    if (parent.empty() || !std::filesystem::exists(parent, ec)) {
      return;
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
        BOLT_MEM_VLOG(1)
            << "ProcessSpillService cleaned stale dir "
            << entry.path().string();
      }
    }
  }
}

} // namespace bytedance::bolt::memory::bm
