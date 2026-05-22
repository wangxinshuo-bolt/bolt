/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/BufferManager.h"

#include <algorithm>
#include <atomic>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryArbitrator.h"
#include "bolt/common/memory/bm/Observability.h"

namespace bytedance::bolt::memory::bm {
namespace {

class BufferManagerReclaimer : public MemoryReclaimer {
 public:
  explicit BufferManagerReclaimer(BufferManager& manager)
      : MemoryReclaimer(0), manager_(manager) {}

  uint64_t reclaim(
      MemoryPool* /*pool*/,
      uint64_t targetBytes,
      uint64_t /*maxWaitMs*/,
      MemoryReclaimer::Stats& /*stats*/) override {
    return manager_.Reclaim(targetBytes);
  }

  void enterArbitration() override {}

  void leaveArbitration() noexcept override {}

  int32_t priority() const override {
    return 0;
  }

  bool reclaimableBytes(
      const MemoryPool& /*pool*/,
      uint64_t& reclaimableBytes) const override {
    reclaimableBytes = manager_.ReclaimableBytes();
    return reclaimableBytes != 0;
  }

  void abort(MemoryPool* /*pool*/, const std::exception_ptr& error) override {
    std::rethrow_exception(error);
  }

 private:
  BufferManager& manager_;
};

std::string uniquePoolName(const std::string& configuredName) {
  static std::atomic<uint64_t> nextId{0};
  if (configuredName.empty()) {
    return fmt::format("__buffer_manager_{}__", nextId++);
  }
  return configuredName;
}

SpillCoordinatorConfig makeProcessSpillConfig(
    const BufferManagerProcessServicesConfig& config) {
  SpillCoordinatorConfig spill;
  spill.spillDir = config.spill.spillDir;
  spill.forcedKind = config.spill.forcedKind;
  spill.workerThreadCount = config.spill.workerThreadCount;
  spill.metrics = config.metrics;
  spill.unknownFallbackKind = config.spill.unknownFallbackKind;
  spill.cleanupOnDestroy = config.spill.cleanupOnDestroy;
  spill.diskProbeDuration = config.spill.diskProbeDuration;
  spill.diskProbe = config.spill.diskProbe;
  spill.diskIo = config.spill.diskIo;
  spill.smallSpill = config.spill.smallSpill;
  spill.compression = config.spill.compression;
  return spill;
}

} // namespace

BufferManager::BufferManager(
    MemoryManager& memoryManager,
    const BufferManagerConfig& config)
    : memoryManager_(memoryManager),
      ownerThreadId_(std::this_thread::get_id()),
      config_(config),
      metrics_(EffectiveMetricsRegistry(config_.metrics)),
      allocateRequestsCounter_(
          metrics_.GetCounter("bm_allocate_requests_total", "")),
      reclaimRequestsCounter_(
          metrics_.GetCounter("bm_reclaim_requests_total", "")),
      reclaimBytesCounter_(metrics_.GetCounter("bm_reclaim_bytes_total", "")),
      usedMemoryGauge_(metrics_.GetGauge("bm_used_memory_bytes", "")),
      pinnedMemoryGauge_(metrics_.GetGauge("bm_pinned_memory_bytes", "")),
      allocateDuration_(metrics_.GetHistogram("bm_allocate_duration_us", "")),
      reclaimDuration_(metrics_.GetHistogram("bm_reclaim_duration_us", "")),
      rootPool_(memoryManager_.addRootPool(
          uniquePoolName(config_.poolName),
          kMaxMemory,
          CreateReclaimer())),
      leafPool_(rootPool_->addLeafChild("blocks")),
      pool_(config_),
      allocator_(pool_, *leafPool_),
      spillOwnerToken_(std::make_shared<SpillOwnerToken>()),
      context_(std::make_shared<BufferManagerContext>(
          BufferManagerContext{
              allocator_, std::nullopt, spillOwnerToken_, ownerThreadId_})),
      evictor_(*this) {
  BOLT_MEM_LOG(INFO) << "BufferManager created pool=" << config_.poolName
                     << " spillEnabled=" << config_.spillEnabled
                     << " reserveWaitTimeoutMs="
                     << config_.reserveWaitTimeout.count();
}

void BufferManager::InitializeProcessServices(
    BufferManagerProcessServicesConfig config) {
  if (config.spill.enabled) {
    SpillCoordinator::ConfigureDefault(makeProcessSpillConfig(config));
  }
}

void BufferManager::ResetProcessServicesForTesting() {
  SpillCoordinator::ResetForTesting();
  DiskIoTaskExecutor::ResetForTesting();
}

BufferManager::~BufferManager() {
  BOLT_MEM_LOG(INFO) << "Destroying BufferManager";
  if (spillCoordinator_.has_value()) {
    for (;;) {
      DrainSpillCompletions();
      DrainPrefetchCompletions();
      if (!spillCoordinator_->get().HasPendingSpills(spillOwnerToken_)) {
        break;
      }
      if (!spillCoordinator_->get().WaitForProgress(
              0, config_.reserveWaitTimeout)) {
        BOLT_MEM_LOG(WARNING)
            << "BufferManager waiting for pending async spill before destroy"
            << " pool=" << config_.poolName;
      }
    }
  }
  std::vector<std::shared_ptr<BlockHandle>> liveBlocks;
  shuttingDown_ = true;
  liveBlocks.reserve(blocks_.size());
  for (auto& weak : blocks_) {
    if (auto block = weak.lock()) {
      liveBlocks.push_back(std::move(block));
    }
  }
  blocks_.clear();
  for (auto& block : liveBlocks) {
    block->InvalidateForManagerDestruction();
  }
  if (context_ != nullptr) {
    context_->valid = false;
    context_->spill.reset();
  }
  evictor_.ClearSpillRequester();
  spillCoordinator_.reset();
  context_.reset();
  leafPool_.reset();
  rootPool_.reset();
}

BufferHandle BufferManager::Allocate(AllocateOptions options) {
  AssertOwnerThread();
  ScopedBmTimer timer(allocateDuration_);
  allocateRequestsCounter_.Add(1);
  BOLT_USER_CHECK_GT(options.size, 0, "Cannot allocate an empty block");
  BOLT_USER_CHECK(
      options.policy != EvictPolicy::kRecompute ||
          static_cast<bool>(options.recoveryFn),
      "kRecompute block requires recoveryFn");
  if (IsSpillPolicy(options.policy)) {
    EnsureSpillCoordinator();
  }
  BOLT_USER_CHECK(!shuttingDown_, "BufferManager is shutting down");

  auto block = std::make_shared<BlockHandle>(context_, options);
  auto memory = allocator_.Allocate(
      options.tag, options.size, BodyReservationKind(options.policy));
  block->InstallMemory(std::move(memory));
  RegisterBlock(block);
  EnqueueEvictionCandidate(block);
  const auto snapshot = pool_.Snapshot();
  usedMemoryGauge_.Set(static_cast<int64_t>(snapshot.usedTotalBytes));
  pinnedMemoryGauge_.Set(static_cast<int64_t>(snapshot.usedPinnedBytes));
  BOLT_MEM_VLOG(1) << "Allocated BufferManager block id=" << block->Id()
                     << " size=" << options.size
                     << " tag=" << ToString(options.tag)
                     << " policy=" << ToString(options.policy)
                     << " priority=" << static_cast<int>(options.priority)
                     << " reservationKind="
                     << ToString(BodyReservationKind(options.policy));
  return BufferHandle(std::move(block), true);
}

std::shared_ptr<BlockHandle> BufferManager::AllocatePersistent(
    AllocateOptions options,
    std::function<void(DataPtr, ByteCount)> init) {
  AssertOwnerThread();
  auto handle = Allocate(std::move(options));
  init(handle.MutableData(), handle.Size());
  auto block = handle.Block();
  handle.Reset();
  return block;
}

BufferHandle BufferManager::Pin(const std::shared_ptr<BlockHandle>& block) {
  AssertOwnerThread();
  if (block == nullptr) {
    return BufferHandle();
  }
  DrainPrefetchCompletionsBeforePin(block);
  return block->Pin();
}

std::unique_ptr<AccountedMemory> BufferManager::AllocateMemory(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  AssertOwnerThread();
  BOLT_USER_CHECK(
      kind == ReservationKind::kNormal || kind == ReservationKind::kPinned,
      "Public AllocateMemory only accepts normal or pinned reservations");
  return allocator_.Allocate(tag, bytes, kind);
}

BufferPoolReservation BufferManager::ReserveMemory(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  AssertOwnerThread();
  BOLT_USER_CHECK(
      kind == ReservationKind::kNormal || kind == ReservationKind::kPinned,
      "Public ReserveMemory only accepts normal or pinned reservations");
  return pool_.Reserve(tag, bytes, kind);
}

ByteCount BufferManager::GetMemoryUsage() const {
  AssertOwnerThread();
  return pool_.GetMemoryUsage();
}

ByteCount BufferManager::GetMemoryUsage(MemoryTag tag) const {
  AssertOwnerThread();
  return pool_.GetMemoryUsage(tag);
}

BufferPoolSnapshot BufferManager::Snapshot() const {
  AssertOwnerThread();
  auto snapshot = pool_.Snapshot();
  std::vector<std::shared_ptr<BlockHandle>> liveBlocks;
  liveBlocks.reserve(blocks_.size());
  for (const auto& weak : blocks_) {
    if (auto block = weak.lock()) {
      liveBlocks.push_back(std::move(block));
    }
  }
  for (const auto& block : liveBlocks) {
    switch (block->State()) {
      case BlockState::kLoaded:
      case BlockState::kLoading:
      case BlockState::kSpilling:
        snapshot.usedLoadedBytes += block->Size();
        break;
      case BlockState::kSpilled:
        snapshot.usedSpilledBytes += block->Size();
        break;
      case BlockState::kInvalid:
      case BlockState::kDiscarded:
      case BlockState::kEvictedRecomputable:
        break;
    }
  }
  return snapshot;
}

ByteCount BufferManager::Reclaim(ByteCount targetBytes) {
  AssertOwnerThread();
  ScopedBmTimer timer(reclaimDuration_);
  reclaimRequestsCounter_.Add(1);
  const auto before = GetMemoryUsage();
  const auto reclaimableBefore = ReclaimableBytes();
  DrainPrefetchCompletions();
  ByteCount reclaimed = DrainSpillCompletions();
  for (auto it = blocks_.begin(); it != blocks_.end();) {
    if (auto block = it->lock()) {
      EnqueueEvictionCandidate(block);
      ++it;
    } else {
      it = blocks_.erase(it);
    }
  }

  size_t scheduledSpillCount = 0;
  EvictionNode node;
  while ((targetBytes == 0 || reclaimed < targetBytes) &&
         evictor_.TryPopAnyCandidate(node)) {
    if (node.cost == EvictionCostClass::kSpill) {
      EnsureSpillCoordinator();
      auto submit = evictor_.TryScheduleEvict(node);
      BOLT_MEM_VLOG(1) << "BufferManager reclaim spill candidate"
                         << " target=" << targetBytes
                         << " result=" << ToString(submit.kind)
                         << " freed=" << submit.freedBytes
                         << " cost=" << ToString(node.cost)
                         << " priority=" << static_cast<int>(node.priority);
      if (submit.kind == EvictResultKind::kScheduled) {
        ++scheduledSpillCount;
      } else if (submit.kind == EvictResultKind::kFailed ||
          submit.kind == EvictResultKind::kBackpressured) {
        BOLT_MEM_LOG(WARNING)
            << "BufferManager async spill submit did not schedule"
            << " target=" << targetBytes
            << " result=" << ToString(submit.kind);
      }
      continue;
    }

    auto result = evictor_.TryEvictNodeSync(node);
    BOLT_MEM_VLOG(1) << "BufferManager reclaim sync candidate"
                       << " target=" << targetBytes
                       << " result=" << ToString(result.kind)
                       << " freed=" << result.freedBytes
                       << " cost=" << ToString(node.cost)
                       << " priority=" << static_cast<int>(node.priority);
    reclaimed += result.freedBytes;
  }

  size_t completedSpillCount = 0;
  while (completedSpillCount < scheduledSpillCount &&
      spillCoordinator_.has_value()) {
    size_t drained = 0;
    reclaimed += DrainSpillCompletions(&drained);
    completedSpillCount += drained;
    if (completedSpillCount >= scheduledSpillCount) {
      break;
    }
    if (!spillCoordinator_->get().WaitForProgress(
            targetBytes == 0 || reclaimed >= targetBytes
                ? 0
                : targetBytes - reclaimed,
            config_.reserveWaitTimeout)) {
      break;
    }
    drained = 0;
    reclaimed += DrainSpillCompletions(&drained);
    completedSpillCount += drained;
  }
  reclaimed += DrainSpillCompletions();
  reclaimBytesCounter_.Add(reclaimed);
  const auto snapshot = pool_.Snapshot();
  usedMemoryGauge_.Set(static_cast<int64_t>(snapshot.usedTotalBytes));
  pinnedMemoryGauge_.Set(static_cast<int64_t>(snapshot.usedPinnedBytes));
  BOLT_MEM_LOG(INFO) << "BufferManager reclaim target=" << targetBytes
                     << " reclaimed=" << reclaimed
                     << " usageBefore=" << before
                     << " usageAfter=" << snapshot.usedTotalBytes
                     << " reclaimableBefore=" << reclaimableBefore
                     << " scheduledSpill=" << scheduledSpillCount;
  return reclaimed;
}

PrefetchResult BufferManager::Prefetch(
    const std::vector<std::shared_ptr<BlockHandle>>& blocks,
    PrefetchOptions options) {
  AssertOwnerThread();
  PrefetchResult result;
  if (blocks.empty()) {
    DrainPrefetchCompletions();
    return result;
  }
  for (const auto& block : blocks) {
    if (block == nullptr) {
      ++result.skippedCount;
      continue;
    }
    const auto state = block->State();
    if (state == BlockState::kLoaded) {
      ++result.alreadyLoadedCount;
      continue;
    }
    if (state != BlockState::kSpilled) {
      ++result.skippedCount;
      continue;
    }
    EnsureSpillCoordinator();
    std::optional<SpillCoordinator::PrefetchRequest> request;
    try {
      request = block->PrepareAsyncPrefetch();
    } catch (...) {
      if (!options.bestEffort) {
        throw;
      }
      ++result.skippedCount;
      continue;
    }
    if (!request.has_value()) {
      ++result.skippedCount;
      continue;
    }
    auto submit = spillCoordinator_->get().SubmitPrefetch(std::move(*request));
    if (submit.kind == EvictResultKind::kScheduled) {
      ++result.submittedCount;
    } else if (submit.kind == EvictResultKind::kBackpressured) {
      block->CommitAsyncPrefetchFailure(block->EvictionSequence());
      ++result.backpressuredCount;
    } else {
      block->CommitAsyncPrefetchFailure(block->EvictionSequence());
      ++result.skippedCount;
    }
  }
  return result;
}

ByteCount BufferManager::ReclaimableBytes() const {
  AssertOwnerThread();
  std::vector<std::shared_ptr<BlockHandle>> blocks;
  blocks.reserve(blocks_.size());
  for (const auto& weak : blocks_) {
    if (auto block = weak.lock()) {
      blocks.push_back(std::move(block));
    }
  }

  ByteCount bytes = 0;
  for (const auto& block : blocks) {
    if (!block->IsPinned() && block->State() == BlockState::kLoaded) {
      bytes += block->Size();
    }
  }
  return bytes;
}

void BufferManager::RegisterBlock(const std::shared_ptr<BlockHandle>& block) {
  BOLT_USER_CHECK(!shuttingDown_, "BufferManager is shutting down");
  blocks_.push_back(block);
}

void BufferManager::EnqueueEvictionCandidate(
    const std::shared_ptr<BlockHandle>& block) {
  if (block == nullptr || block->options_.policy == EvictPolicy::kPinnedForever) {
    return;
  }
  auto node = MakeEvictionNode(block);
  BOLT_MEM_VLOG(1) << "BufferManager enqueue eviction candidate"
                     << " block_id=" << block->Id()
                     << " cost=" << ToString(node.cost)
                     << " priority=" << static_cast<int>(node.priority)
                     << " sequence=" << node.evictionSequence
                     << " policy=" << ToString(block->options_.policy);
  evictor_.Enqueue(std::move(node));
}

EvictionNode BufferManager::MakeEvictionNode(
    const std::shared_ptr<BlockHandle>& block) {
  EvictionNode node;
  node.block = std::weak_ptr<BlockHandleBase>(
      std::static_pointer_cast<BlockHandleBase>(block));
  node.evictionSequence = block->EvictionSequence();
  node.cost = EvictionCostFor(block->options_.policy, block->State());
  node.priority = block->options_.priority;
  return node;
}

std::unique_ptr<MemoryReclaimer> BufferManager::CreateReclaimer() {
  return std::make_unique<BufferManagerReclaimer>(*this);
}

void BufferManager::AssertOwnerThread() const {
  BOLT_USER_CHECK(
      std::this_thread::get_id() == ownerThreadId_,
      "BufferManager is thread-confined: public API must be called from the "
      "thread that constructed it");
}

ByteCount BufferManager::DrainSpillCompletions(size_t* completionCount) {
  if (completionCount != nullptr) {
    *completionCount = 0;
  }
  if (!spillCoordinator_.has_value()) {
    return 0;
  }
  ByteCount reclaimed = 0;
  auto completions =
      spillCoordinator_->get().DrainCompletions(spillOwnerToken_);
  if (completionCount != nullptr) {
    *completionCount = completions.size();
  }
  for (auto& completion : completions) {
    auto block = FindBlockById(completion.blockId);
    const auto memoryBytes =
        completion.memory == nullptr ? 0 : completion.memory->Size();
    if (completion.error.empty() && completion.location.Valid()) {
      if (block != nullptr) {
        reclaimed += block->CommitAsyncSpillSuccess(
            completion.evictionSequence,
            std::move(completion.location),
            std::move(completion.memory));
      } else {
        spillCoordinator_->get().Release(completion.location);
        completion.memory.reset();
        reclaimed += memoryBytes;
        BOLT_MEM_VLOG(1)
            << "BufferManager dropped async spill completion for dead block_id="
            << completion.blockId << " freed=" << memoryBytes;
      }
    } else {
      if (block != nullptr) {
        block->CommitAsyncSpillFailure(
            completion.evictionSequence, std::move(completion.memory));
      } else {
        completion.memory.reset();
        reclaimed += memoryBytes;
      }
      BOLT_MEM_LOG(WARNING) << "BufferManager async spill failed block_id="
                            << completion.blockId
                            << " error=" << completion.error;
    }
  }
  return reclaimed;
}

ByteCount BufferManager::DrainPrefetchCompletions(size_t* completionCount) {
  if (completionCount != nullptr) {
    *completionCount = 0;
  }
  if (!spillCoordinator_.has_value()) {
    return 0;
  }
  ByteCount loaded = 0;
  auto completions =
      spillCoordinator_->get().DrainPrefetchCompletions(spillOwnerToken_);
  if (completionCount != nullptr) {
    *completionCount = completions.size();
  }
  for (auto& completion : completions) {
    auto block = FindBlockById(completion.blockId);
    if (completion.error.empty()) {
      if (block != nullptr) {
        loaded += block->CommitAsyncPrefetchSuccess(
            completion.evictionSequence, std::move(completion.memory));
        if (block->State() == BlockState::kLoaded && !block->IsPinned()) {
          EnqueueEvictionCandidate(block);
        }
      } else {
        completion.memory.reset();
        BOLT_MEM_VLOG(1)
            << "BufferManager dropped async prefetch completion for dead "
               "block_id="
            << completion.blockId;
      }
    } else {
      if (block != nullptr) {
        block->CommitAsyncPrefetchFailure(completion.evictionSequence);
      }
      completion.memory.reset();
      BOLT_MEM_LOG(WARNING) << "BufferManager async prefetch failed block_id="
                            << completion.blockId
                            << " error=" << completion.error;
    }
  }
  return loaded;
}

void BufferManager::DrainPrefetchCompletionsBeforePin(
    const std::shared_ptr<BlockHandle>& block) {
  if (block == nullptr) {
    return;
  }
  for (;;) {
    DrainPrefetchCompletions();
    if (block->State() != BlockState::kLoading) {
      return;
    }
    if (!spillCoordinator_.has_value() ||
        !spillCoordinator_->get().HasPendingSpills(spillOwnerToken_)) {
      return;
    }
    if (!spillCoordinator_->get().WaitForProgress(
            0, config_.reserveWaitTimeout)) {
      BOLT_MEM_LOG(WARNING)
          << "BufferManager waiting for async prefetch before pin"
          << " pool=" << config_.poolName << " block_id=" << block->Id();
    }
  }
}

std::shared_ptr<BlockHandle> BufferManager::FindBlockById(uint64_t blockId) {
  for (auto it = blocks_.begin(); it != blocks_.end();) {
    auto block = it->lock();
    if (block == nullptr) {
      it = blocks_.erase(it);
      continue;
    }
    if (block->Id() == blockId) {
      return block;
    }
    ++it;
  }
  return nullptr;
}

void BufferManager::EnsureSpillCoordinator() {
  if (spillCoordinator_.has_value()) {
    return;
  }
  BOLT_USER_CHECK(
      config_.spillEnabled,
      "BufferManager spill is disabled but kSpillToDisk was requested");
  spillCoordinator_ = SpillCoordinator::Instance();
  context_->spill = spillCoordinator_;
  evictor_.SetSpillRequester(spillCoordinator_->get());
}

} // namespace bytedance::bolt::memory::bm
