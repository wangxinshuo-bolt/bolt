/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillCoordinator.h"

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
  SpillCoordinatorConfig pendingConfig;
  std::unique_ptr<SpillCoordinator> instance;
};

SingletonState& globalState() {
  static SingletonState state;
  return state;
}

void validateSpillCoordinatorConfig(const SpillCoordinatorConfig& config) {
  BOLT_USER_CHECK(
      config.workerThreadCount > 0,
      "BufferManager spill requires workerThreadCount > 0");
}

} // namespace

SpillCoordinator::SpillCoordinator(SpillCoordinatorConfig config)
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
  validateSpillCoordinatorConfig(config_);
  BOLT_USER_CHECK(
      !config_.spillDir.empty(),
      "SpillCoordinator requires a spill directory");
  CleanupStaleDirsAtStartup();
  SpillFileStoreConfig storeCfg;
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
  auto diskIoConfig = config_.diskIo;
  diskIoConfig.workerThreadCount = config_.workerThreadCount;
  DiskIoTaskExecutor::ConfigureDefaultIfNeeded(diskIoConfig);
  SpillFileStore::CleanupAtStartup(storeCfg);
  store_ = std::make_unique<SpillFileStore>(storeCfg, config_.metrics);
  BOLT_MEM_LOG(INFO) << "SpillCoordinator initialized"
                     << " spillDir=" << config_.spillDir
                     << " workerThreadCount=" << config_.workerThreadCount
                     << " cleanupOnDestroy=" << config_.cleanupOnDestroy
                     << " diskKind=" << ToString(storeCfg.diskProbe.kind)
                     << " probeActive=" << storeCfg.diskProbe.activeProbeRan
                     << " writeIops=" << storeCfg.diskProbe.writeIops
                     << " readIops=" << storeCfg.diskProbe.readIops;
}

SpillCoordinator::~SpillCoordinator() {
  StopAsyncTasks();
  store_.reset();
}

SpillCoordinator& SpillCoordinator::Instance() {
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  if (state.instance != nullptr) {
    return *state.instance;
  }
  BOLT_USER_CHECK(
      state.configured,
      "SpillCoordinator::Instance() called without ConfigureDefault");
  state.instance.reset(
      new SpillCoordinator(std::move(state.pendingConfig)));
  return *state.instance;
}

void SpillCoordinator::ConfigureDefault(SpillCoordinatorConfig config) {
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  BOLT_USER_CHECK(
      !state.configured && state.instance == nullptr,
      "SpillCoordinator::ConfigureDefault may be called only once");
  BOLT_USER_CHECK(
      !config.spillDir.empty(),
      "SpillCoordinatorConfig.spillDir must not be empty");
  validateSpillCoordinatorConfig(config);
  state.pendingConfig = std::move(config);
  state.configured = true;
}

void SpillCoordinator::ResetForTesting() {
  auto& state = globalState();
  std::unique_ptr<SpillCoordinator> dying;
  {
    std::lock_guard<std::mutex> l(state.mutex);
    dying = std::move(state.instance);
    state.configured = false;
    state.pendingConfig = SpillCoordinatorConfig{};
  }
  // Destroy outside the singleton mutex so worker threads can join cleanly.
  dying.reset();
}

std::unique_ptr<SpillCoordinator> SpillCoordinator::CreateForTesting(
    SpillCoordinatorConfig config) {
  return std::unique_ptr<SpillCoordinator>(
      new SpillCoordinator(std::move(config)));
}

EvictResult SpillCoordinator::SubmitSpill(EvictionNode node) {
  ScopedBmTimer timer(submitDuration_);
  submitCounter_.Add(1);
  if (node.block.expired()) {
    skippedCounter_.Add(1);
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  auto base = node.block.lock();
  if (base == nullptr) {
    skippedCounter_.Add(1);
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  auto handle = std::dynamic_pointer_cast<BlockHandle>(base);
  if (handle == nullptr) {
    skippedCounter_.Add(1);
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  auto request = handle->PrepareAsyncSpill(node.evictionSequence);
  if (!request.has_value()) {
    skippedCounter_.Add(1);
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopping_) {
      handle->CommitAsyncSpillFailure(
          node.evictionSequence, std::move(request->memory));
      failedCounter_.Add(1);
      return EvictResult{EvictResultKind::kFailed, 0};
    }
  }
  auto spillRequest =
      std::make_shared<SpillRequest>(std::move(*request));
  AddActiveOwner(spillRequest->owner);
  const bool submitted = DiskIoTaskExecutor::Instance().SubmitTask(DiskIoTask{
      DiskIoPriority::kHigh,
      [this, request = spillRequest]() mutable {
        ExecuteSpill(std::move(*request));
        NotifyProgress();
      }});
  if (!submitted) {
    CompleteActiveOwner(spillRequest->owner);
    handle->CommitAsyncSpillFailure(
        node.evictionSequence, std::move(spillRequest->memory));
    failedCounter_.Add(1);
    return EvictResult{EvictResultKind::kFailed, 0};
  }
  queueDepthGauge_.Set(0);
  scheduledCounter_.Add(1);
  BOLT_MEM_VLOG(1) << "SpillCoordinator scheduled spill submit"
                     << " queueDepth=central_disk_io";
  return EvictResult{EvictResultKind::kScheduled, 0};
}

bool SpillCoordinator::WaitForProgress(
    ByteCount /*bytesNeeded*/,
    std::chrono::milliseconds timeout) {
  ScopedBmTimer timer(waitForProgressDuration_);
  std::unique_lock<std::mutex> l(mutex_);
  const auto startEpoch = progressEpoch_;
  return progressCv_.wait_for(
      l, timeout, [&] { return stopping_ || progressEpoch_ != startEpoch; });
}

std::vector<SpillCoordinator::SpillCompletion>
SpillCoordinator::DrainCompletions(
    const std::shared_ptr<SpillOwnerToken>& owner) {
  std::vector<SpillCompletion> completions;
  std::lock_guard<std::mutex> l(mutex_);
  for (auto it = completed_.begin(); it != completed_.end();) {
    if (!it->owner.owner_before(owner) && !owner.owner_before(it->owner)) {
      completions.push_back(std::move(*it));
      it = completed_.erase(it);
    } else {
      ++it;
    }
  }
  return completions;
}

std::vector<SpillCoordinator::PrefetchCompletion>
SpillCoordinator::DrainPrefetchCompletions(
    const std::shared_ptr<SpillOwnerToken>& owner) {
  std::vector<PrefetchCompletion> completions;
  std::lock_guard<std::mutex> l(mutex_);
  for (auto it = prefetchCompleted_.begin(); it != prefetchCompleted_.end();) {
    if (!it->owner.owner_before(owner) && !owner.owner_before(it->owner)) {
      completions.push_back(std::move(*it));
      it = prefetchCompleted_.erase(it);
    } else {
      ++it;
    }
  }
  return completions;
}

bool SpillCoordinator::HasPendingSpills(
    const std::shared_ptr<SpillOwnerToken>& owner) const {
  std::lock_guard<std::mutex> l(mutex_);
  for (const auto& completion : completed_) {
    if (!completion.owner.owner_before(owner) &&
        !owner.owner_before(completion.owner)) {
      return true;
    }
  }
  for (const auto& completion : prefetchCompleted_) {
    if (!completion.owner.owner_before(owner) &&
        !owner.owner_before(completion.owner)) {
      return true;
    }
  }
  auto active = activeByOwner_.find(owner);
  return active != activeByOwner_.end() && active->second != 0;
}

SpillLocation SpillCoordinator::Write(
    MemoryTag tag,
    ConstDataPtr src,
    ByteCount bytes) {
  auto location = store_->Write(tag, src, bytes);
  usedDiskBytes_.fetch_add(
      location.slotBytes == 0 ? location.storedBytes : location.slotBytes,
      std::memory_order_relaxed);
  return location;
}

void SpillCoordinator::Read(
    const SpillLocation& location,
    DataPtr dst,
    ByteCount dstCapacity) {
  store_->Read(location, dst, dstCapacity);
}

void SpillCoordinator::Release(const SpillLocation& location) noexcept {
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

void SpillCoordinator::StopAsyncTasks() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopping_) {
      return;
    }
    stopping_ = true;
    queueDepthGauge_.Set(0);
  }
  for (;;) {
    std::unique_lock<std::mutex> l(mutex_);
    if (activeByOwner_.empty()) {
      break;
    }
    progressCv_.wait_for(
        l,
        std::chrono::milliseconds(100),
        [&] { return activeByOwner_.empty(); });
  }
  NotifyProgress();
}

void SpillCoordinator::ExecuteSpill(SpillRequest request) {
  ScopedBmTimer timer(executeDuration_);
  SpillCompletion completion;
  completion.owner = request.owner;
  completion.blockId = request.blockId;
  completion.evictionSequence = request.evictionSequence;
  completion.tag = request.tag;
  completion.memory = std::move(request.memory);
  try {
    BOLT_USER_CHECK_NOT_NULL(
        completion.memory, "Async spill request has no resident memory");
    const auto bytes = completion.memory->Size();
    completion.location =
        Write(completion.tag, completion.memory->Data(), bytes);
    executedCounter_.Add(1);
    freedBytesCounter_.Add(bytes);
    BOLT_MEM_VLOG(1) << "SpillCoordinator wrote async spill bytes="
                     << bytes;
  } catch (const std::exception& e) {
    completion.error = e.what();
    failedCounter_.Add(1);
    BOLT_MEM_LOG(WARNING) << "SpillCoordinator failed async spill: "
                          << e.what();
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    completed_.push_back(std::move(completion));
  }
  CompleteActiveOwner(request.owner);
}

EvictResult SpillCoordinator::SubmitPrefetch(PrefetchRequest request) {
  if (request.owner.expired()) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  BOLT_USER_CHECK_NOT_NULL(
      request.memory, "SpillCoordinator prefetch requires resident memory");
  BOLT_USER_CHECK(
      request.location.Valid(),
      "SpillCoordinator prefetch requires a valid spill location");
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopping_) {
      return EvictResult{EvictResultKind::kFailed, 0};
    }
  }
  auto prefetchRequest =
      std::make_shared<PrefetchRequest>(std::move(request));
  AddActiveOwner(prefetchRequest->owner);
  const bool submitted = DiskIoTaskExecutor::Instance().SubmitTask(DiskIoTask{
      DiskIoPriority::kLow,
      [this, request = prefetchRequest]() mutable {
        ExecutePrefetch(std::move(*request));
        NotifyProgress();
      }});
  if (!submitted) {
    CompleteActiveOwner(prefetchRequest->owner);
    return EvictResult{EvictResultKind::kFailed, 0};
  }
  queueDepthGauge_.Set(0);
  return EvictResult{EvictResultKind::kScheduled, 0};
}

void SpillCoordinator::ExecutePrefetch(PrefetchRequest request) {
  ScopedBmTimer timer(executeDuration_);
  PrefetchCompletion completion;
  completion.owner = request.owner;
  completion.blockId = request.blockId;
  completion.evictionSequence = request.evictionSequence;
  completion.tag = request.tag;
  completion.memory = std::move(request.memory);
  try {
    BOLT_USER_CHECK_NOT_NULL(
        completion.memory, "Async prefetch request has no resident memory");
    Read(request.location, completion.memory->Data(), completion.memory->Size());
    executedCounter_.Add(1);
    BOLT_MEM_VLOG(1) << "SpillCoordinator read async prefetch bytes="
                     << completion.memory->Size();
  } catch (const std::exception& e) {
    completion.error = e.what();
    failedCounter_.Add(1);
    BOLT_MEM_LOG(WARNING) << "SpillCoordinator failed async prefetch: "
                          << e.what();
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    prefetchCompleted_.push_back(std::move(completion));
  }
  CompleteActiveOwner(request.owner);
}

void SpillCoordinator::AddActiveOwner(
    const std::weak_ptr<SpillOwnerToken>& owner) {
  if (auto ownerToken = owner.lock()) {
    std::lock_guard<std::mutex> l(mutex_);
    ++activeByOwner_[ownerToken];
  }
}

void SpillCoordinator::CompleteActiveOwner(
    const std::weak_ptr<SpillOwnerToken>& owner) {
  if (auto ownerToken = owner.lock()) {
    std::lock_guard<std::mutex> l(mutex_);
    auto active = activeByOwner_.find(ownerToken);
    if (active != activeByOwner_.end()) {
      if (active->second <= 1) {
        activeByOwner_.erase(active);
      } else {
        --active->second;
      }
    }
  }
}

void SpillCoordinator::NotifyProgress() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++progressEpoch_;
  }
  progressCv_.notify_all();
}

void SpillCoordinator::CleanupStaleDirsAtStartup() {
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
            << "SpillCoordinator failed to clean stale dir "
            << entry.path().string() << ": " << rmEc.message();
      } else {
        BOLT_MEM_VLOG(1)
            << "SpillCoordinator cleaned stale dir "
            << entry.path().string();
      }
    }
  }
}

} // namespace bytedance::bolt::memory::bm
