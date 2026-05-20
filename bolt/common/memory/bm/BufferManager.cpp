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

} // namespace

BufferManager::BufferManager(
    MemoryManager& memoryManager,
    BufferManagerConfig config)
    : memoryManager_(memoryManager),
      config_(std::move(config)),
      rootPool_(memoryManager_.addRootPool(
          uniquePoolName(config_.poolName),
          kMaxMemory,
          CreateReclaimer())),
      leafPool_(rootPool_->addLeafChild("blocks")),
      pool_(config_),
      allocator_(pool_, *leafPool_),
      evictor_(*this) {
  BOLT_MEM_LOG(INFO) << "BufferManager created pool=" << config_.poolName
                     << " spillEnabled=" << (spillService_ != nullptr);
}

BufferManager::~BufferManager() {
  BOLT_MEM_LOG(INFO) << "Destroying BufferManager";
  std::vector<std::shared_ptr<BlockHandle>> liveBlocks;
  {
    std::lock_guard<std::mutex> l(mutex_);
    shuttingDown_ = true;
    liveBlocks.reserve(blocks_.size());
    for (auto& weak : blocks_) {
      if (auto block = weak.lock()) {
        liveBlocks.push_back(std::move(block));
      }
    }
    blocks_.clear();
  }
  for (auto& block : liveBlocks) {
    block->InvalidateForManagerDestruction();
  }
  evictor_.SetSpillRequester(nullptr);
  spillService_ = nullptr;
  leafPool_.reset();
  rootPool_.reset();
}

BufferHandle BufferManager::Allocate(AllocateOptions options) {
  BOLT_USER_CHECK_GT(options.size, 0, "Cannot allocate an empty block");
  BOLT_USER_CHECK(
      options.policy != EvictPolicy::kRecompute ||
          static_cast<bool>(options.recoveryFn),
      "kRecompute block requires recoveryFn");
  if (IsSpillPolicy(options.policy)) {
    EnsureSpillService();
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    BOLT_USER_CHECK(!shuttingDown_, "BufferManager is shutting down");
  }

  auto block = std::make_shared<BlockHandle>(this, options);
  auto memory = allocator_.Allocate(
      options.tag, options.size, BodyReservationKind(options.policy));
  block->InstallMemory(std::move(memory));
  RegisterBlock(block);
  EnqueueEvictionCandidate(block);
  BOLT_MEM_LOG(INFO) << "Allocated BufferManager block id=" << block->Id()
                     << " size=" << options.size
                     << " policy=" << ToString(options.policy);
  return BufferHandle(std::move(block), true);
}

std::shared_ptr<BlockHandle> BufferManager::AllocatePersistent(
    AllocateOptions options,
    std::function<void(DataPtr, ByteCount)> init) {
  auto handle = Allocate(std::move(options));
  init(handle.MutableData(), handle.Size());
  auto block = handle.Block();
  handle.Reset();
  return block;
}

BufferHandle BufferManager::Pin(const std::shared_ptr<BlockHandle>& block) {
  if (block == nullptr) {
    return BufferHandle();
  }
  return block->Pin();
}

std::unique_ptr<AccountedMemory> BufferManager::AllocateMemory(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  BOLT_USER_CHECK(
      kind == ReservationKind::kNormal || kind == ReservationKind::kPinned,
      "Public AllocateMemory only accepts normal or pinned reservations");
  return allocator_.Allocate(tag, bytes, kind);
}

BufferPoolReservation BufferManager::ReserveMemory(
    MemoryTag tag,
    ByteCount bytes,
    ReservationKind kind) {
  BOLT_USER_CHECK(
      kind == ReservationKind::kNormal || kind == ReservationKind::kPinned,
      "Public ReserveMemory only accepts normal or pinned reservations");
  return pool_.Reserve(tag, bytes, kind);
}

ByteCount BufferManager::GetMemoryUsage() const {
  return pool_.GetMemoryUsage();
}

ByteCount BufferManager::GetMemoryUsage(MemoryTag tag) const {
  return pool_.GetMemoryUsage(tag);
}

BufferPoolSnapshot BufferManager::Snapshot() const {
  return pool_.Snapshot();
}

ByteCount BufferManager::Reclaim(ByteCount targetBytes) {
  const auto before = GetMemoryUsage();
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto it = blocks_.begin(); it != blocks_.end();) {
      if (auto block = it->lock()) {
        EnqueueEvictionCandidate(block);
        ++it;
      } else {
        it = blocks_.erase(it);
      }
    }
  }

  ByteCount reclaimed = 0;
  bool scheduledSpill = false;
  EvictionNode node;
  while (evictor_.TryPopAnyCandidate(node)) {
    if (targetBytes != 0 && reclaimed >= targetBytes) {
      break;
    }

    if (node.cost == EvictionCostClass::kSpill) {
      auto submit = evictor_.TryScheduleEvict(node);
      if (submit.kind == EvictResultKind::kScheduled) {
        scheduledSpill = true;
        continue;
      }
      auto base = node.block.lock();
      auto block = std::dynamic_pointer_cast<BlockHandle>(base);
      if (block != nullptr) {
        reclaimed += block->SpillToDisk();
      }
      continue;
    }

    auto result = evictor_.TryEvictNodeSync(node);
    reclaimed += result.freedBytes;
  }

  if (scheduledSpill && (targetBytes == 0 || reclaimed < targetBytes) &&
      spillService_ != nullptr) {
    // A scheduled spill is not freed memory. Wait for at least one progress
    // event and then compute the actual usage delta. This mirrors the design
    // rule that only committed spill/release can be counted as reclaimed.
    spillService_->WaitForProgress(
        targetBytes == 0 ? 0 : targetBytes - reclaimed,
        config_.reserveWaitTimeout);
    const auto after = GetMemoryUsage();
    reclaimed = std::max(reclaimed, before > after ? before - after : 0);
  }
  BOLT_MEM_LOG(INFO) << "BufferManager reclaim target=" << targetBytes
                     << " reclaimed=" << reclaimed
                     << " usageBefore=" << before
                     << " usageAfter=" << GetMemoryUsage();
  return reclaimed;
}

ByteCount BufferManager::ReclaimableBytes() const {
  std::vector<std::shared_ptr<BlockHandle>> blocks;
  {
    std::lock_guard<std::mutex> l(mutex_);
    blocks.reserve(blocks_.size());
    for (const auto& weak : blocks_) {
      if (auto block = weak.lock()) {
        blocks.push_back(std::move(block));
      }
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
  std::lock_guard<std::mutex> l(mutex_);
  BOLT_USER_CHECK(!shuttingDown_, "BufferManager is shutting down");
  blocks_.push_back(block);
}

void BufferManager::EnqueueEvictionCandidate(
    const std::shared_ptr<BlockHandle>& block) {
  if (block == nullptr || block->options_.policy == EvictPolicy::kPinnedForever) {
    return;
  }
  evictor_.Enqueue(MakeEvictionNode(block));
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

void BufferManager::EnsureSpillService() {
  if (spillService_ != nullptr) {
    return;
  }
  spillService_ = &ProcessSpillService::Instance();
  evictor_.SetSpillRequester(spillService_);
}

} // namespace bytedance::bolt::memory::bm
