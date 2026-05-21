/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/BlockHandle.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

namespace bytedance::bolt::memory::bm {
namespace {

uint64_t nextBlockId() {
  static std::atomic<uint64_t> nextId{1};
  return nextId.fetch_add(1, std::memory_order_relaxed);
}

std::shared_ptr<BufferManagerContext> lockContext(
    const std::weak_ptr<BufferManagerContext>& context) {
  auto locked = context.lock();
  BOLT_USER_CHECK(
      locked != nullptr && locked->valid,
      "BufferManager no longer owns this block");
  return locked;
}

std::shared_ptr<BufferManagerContext> lockOwnerContext(
    const std::weak_ptr<BufferManagerContext>& context) {
  auto locked = lockContext(context);
  BOLT_USER_CHECK(
      std::this_thread::get_id() == locked->ownerThreadId,
      "BufferManager block is thread-confined: direct block operations must "
      "run on the BufferManager owner thread");
  return locked;
}

ProcessSpillService& requireSpill(BufferManagerContext& context) {
  BOLT_USER_CHECK(
      context.spill.has_value(),
      "BufferManager spill service is not wired");
  return context.spill->get();
}

} // namespace

BlockHandle::BlockHandle(
    std::weak_ptr<BufferManagerContext> context,
    AllocateOptions options)
    : id_(nextBlockId()),
      context_(std::move(context)),
      options_(std::move(options)),
      size_(options_.size) {
  BOLT_MEM_VLOG(1) << "Created BufferManager block id=" << id_
                     << " size=" << size_
                     << " policy=" << ToString(options_.policy)
                     << " tag=" << ToString(options_.tag);
}

BlockHandle::~BlockHandle() {
  InvalidateForManagerDestruction();
}

BufferHandle BlockHandle::Pin() {
  std::unique_ptr<AccountedMemory> loaded;
  std::optional<SpillLocation> oldSpill;
  ProcessSpillService* spillForRelease{nullptr};
  std::optional<SpillLocation> spillToRelease;
  uint64_t myGeneration = 0;
  {
    std::unique_lock<std::mutex> l(mutex_);
    for (;;) {
      if (state_ == BlockState::kLoaded) {
        if (spillScheduled_) {
          spillScheduled_ = false;
          ++evictionSequence_;
        }
        ++pinCount_;
        BOLT_MEM_VLOG(1) << "Pinned resident BufferManager block id=" << id_
                           << " pinCount=" << pinCount_;
        return BufferHandle(shared_from_this(), false);
      }
      if (state_ == BlockState::kDiscarded || state_ == BlockState::kInvalid) {
        BOLT_MEM_VLOG(1) << "Pin returned invalid for BufferManager block id="
                           << id_ << " state=" << ToString(state_);
        return BufferHandle();
      }
      if (state_ == BlockState::kSpilling || state_ == BlockState::kLoading) {
        // Snapshot the generation we are observing so that, if loading fails,
        // we propagate the same error to all waiters instead of each thread
        // starting its own reload.
        const uint64_t observedGeneration =
            state_ == BlockState::kLoading ? loadGeneration_ : 0;
        cv_.wait(l);
        if (lastLoadError_ && loadGeneration_ == observedGeneration &&
            observedGeneration != 0) {
          std::rethrow_exception(lastLoadError_);
        }
        continue;
      }
      break;
    }
    BOLT_USER_CHECK(
        state_ == BlockState::kEvictedRecomputable ||
            state_ == BlockState::kSpilled,
        "Unsupported BufferManager block state");
    if (state_ == BlockState::kSpilled) {
      oldSpill = spillLocation_;
    }
    BOLT_MEM_VLOG(1) << "Loading BufferManager block id=" << id_
                       << " from state=" << ToString(state_);
    state_ = BlockState::kLoading;
    myGeneration = ++loadGeneration_;
    lastLoadError_ = nullptr;
  }

  try {
    auto context = lockContext(context_);
    loaded = context->allocator.Allocate(
        options_.tag, size_, BodyReservationKind(options_.policy));
    if (oldSpill.has_value()) {
      requireSpill(*context).Read(*oldSpill, loaded->Data(), loaded->Size());
    } else if (options_.recoveryFn) {
      options_.recoveryFn(loaded->Data(), loaded->Size());
    }
  } catch (...) {
    auto error = std::current_exception();
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (state_ == BlockState::kLoading &&
          loadGeneration_ == myGeneration) {
        if (oldSpill.has_value()) {
          state_ = BlockState::kSpilled;
        } else {
          state_ = BlockState::kEvictedRecomputable;
        }
        lastLoadError_ = error;
      }
    }
    cv_.notify_all();
    BOLT_MEM_LOG(WARNING) << "Failed to load BufferManager block id=" << id_;
    std::rethrow_exception(error);
  }

  {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ == BlockState::kInvalid || state_ == BlockState::kDiscarded) {
      return BufferHandle();
    }
    memory_ = std::move(loaded);
    state_ = BlockState::kLoaded;
    auto context = context_.lock();
    if (oldSpill.has_value() && context != nullptr && context->valid &&
        context->spill.has_value()) {
      spillForRelease = &context->spill->get();
      spillToRelease = *oldSpill;
      spillLocation_ = SpillLocation{};
    }
    ++pinCount_;
    BOLT_MEM_VLOG(1) << "Loaded and pinned BufferManager block id=" << id_
                       << " pinCount=" << pinCount_;
  }
  cv_.notify_all();
  if (spillForRelease != nullptr && spillToRelease.has_value()) {
    spillForRelease->Release(*spillToRelease);
  }
  return BufferHandle(shared_from_this(), false);
}

ByteCount BlockHandle::TryEvict(ByteCount targetBytes) {
  std::unique_ptr<AccountedMemory> evicted;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ != BlockState::kLoaded || pinCount_ != 0 || memory_ == nullptr ||
        options_.policy == EvictPolicy::kPinnedForever) {
      return 0;
    }
    if (IsSpillPolicy(options_.policy)) {
      // Spill is handled outside this lock by SpillToDisk() so that file I/O
      // never blocks unrelated block state checks.
      return 0;
    }
    if (options_.policy == EvictPolicy::kRecompute) {
      BOLT_USER_CHECK(
          static_cast<bool>(options_.recoveryFn),
          "kRecompute block requires recoveryFn");
      state_ = BlockState::kEvictedRecomputable;
    } else {
      state_ = BlockState::kDiscarded;
    }
    evicted = std::move(memory_);
  }
  const auto freed = evicted == nullptr ? 0 : evicted->Size();
  evicted.reset();
  cv_.notify_all();
  BOLT_MEM_VLOG(1) << "Evicted BufferManager block id=" << id_
                     << " policy=" << ToString(options_.policy)
                     << " freed=" << freed;
  return freed;
}

ByteCount BlockHandle::SpillToDisk() {
  std::unique_ptr<AccountedMemory> spillingMemory;
  std::optional<std::reference_wrapper<ProcessSpillService>> spill;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!IsSpillPolicy(options_.policy) || state_ != BlockState::kLoaded ||
        pinCount_ != 0) {
      spillScheduled_ = false;
      return 0;
    }
    if (memory_ == nullptr) {
      spillScheduled_ = false;
      return 0;
    }
    auto context = lockOwnerContext(context_);
    spill = requireSpill(*context);
    // Move ownership out of the block while I/O runs so manager destruction
    // cannot free the buffer underneath the write. The AccountedMemory remains
    // alive and counted until the write commits.
    state_ = BlockState::kSpilling;
    spillingMemory = std::move(memory_);
    BOLT_MEM_VLOG(1) << "Spilling BufferManager block id=" << id_
                       << " size=" << spillingMemory->Size()
                       << " policy=" << ToString(options_.policy);
  }

  SpillLocation location;
  try {
    location = spill->get().Write(
        options_.tag, spillingMemory->Data(), spillingMemory->Size());
  } catch (...) {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ == BlockState::kSpilling) {
      memory_ = std::move(spillingMemory);
      state_ = BlockState::kLoaded;
      spillScheduled_ = false;
      // Spill failure changes candidate identity, so in-flight EvictionNode
      // snapshots must become stale before a fresh node is enqueued.
      ++evictionSequence_;
    }
    cv_.notify_all();
    BOLT_MEM_LOG(WARNING) << "Failed to spill BufferManager block id=" << id_
                          << ", restored to "
                          << ToString(state_);
    throw;
  }

  std::unique_ptr<AccountedMemory> evicted;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto context = context_.lock();
    if (state_ != BlockState::kSpilling || context == nullptr ||
        !context->valid) {
      // BufferManager destruction (or a racing invalidate) raced ahead of
      // this committed write. Drop the spill file we just produced so it
      // does not leak.
      spill->get().Release(location);
      cv_.notify_all();
      return 0;
    }
    spillLocation_ = std::move(location);
    evicted = std::move(spillingMemory);
    state_ = BlockState::kSpilled;
    spillScheduled_ = false;
  }
  const auto freed = evicted == nullptr ? 0 : evicted->Size();
  evicted.reset();
  cv_.notify_all();
  BOLT_MEM_VLOG(1) << "Spilled BufferManager block id=" << id_
                     << " freed=" << freed
                     << " file=" << spillLocation_.path;
  return freed;
}

void BlockHandle::InvalidateForManagerDestruction() noexcept {
  std::unique_ptr<AccountedMemory> old;
  std::optional<std::reference_wrapper<ProcessSpillService>> spill;
  SpillLocation locationToRelease;
  {
    std::lock_guard<std::mutex> l(mutex_);
    state_ = BlockState::kInvalid;
    spillScheduled_ = false;
    old = std::move(memory_);
    if (auto context = context_.lock()) {
      if (context->spill.has_value() && spillLocation_.Valid()) {
        spill = context->spill;
        locationToRelease = std::move(spillLocation_);
        spillLocation_ = SpillLocation{};
      }
    }
    pinCount_ = 0;
    // Wake any kLoading/kSpilling waiter and synthesize a generation-bumping
    // error so they do not retry against a torn-down manager.
    ++loadGeneration_;
    try {
      throw std::runtime_error("BufferManager destroyed during reload");
    } catch (...) {
      lastLoadError_ = std::current_exception();
    }
  }
  if (locationToRelease.Valid() && spill.has_value()) {
    spill->get().Release(locationToRelease);
  }
  old.reset();
  cv_.notify_all();
}

bool BlockHandle::TryMarkSpillScheduled(uint64_t expectedSequence) {
  std::lock_guard<std::mutex> l(mutex_);
  if (!IsSpillPolicy(options_.policy) ||
      evictionSequence_ != expectedSequence ||
      state_ != BlockState::kLoaded || pinCount_ != 0 || spillScheduled_ ||
      context_.expired()) {
    return false;
  }
  spillScheduled_ = true;
  return true;
}

std::optional<ProcessSpillService::SpillRequest> BlockHandle::PrepareAsyncSpill(
    uint64_t expectedSequence) {
  std::lock_guard<std::mutex> l(mutex_);
  if (!IsSpillPolicy(options_.policy) ||
      evictionSequence_ != expectedSequence ||
      state_ != BlockState::kLoaded || pinCount_ != 0 ||
      !spillScheduled_ || memory_ == nullptr) {
    return std::nullopt;
  }
  auto context = context_.lock();
  if (context == nullptr || !context->valid) {
    return std::nullopt;
  }
  state_ = BlockState::kSpilling;
  spillScheduled_ = true;
  ProcessSpillService::SpillRequest request;
  request.owner = context->spillOwnerToken;
  request.blockId = id_;
  request.evictionSequence = expectedSequence;
  request.tag = options_.tag;
  request.memory = std::move(memory_);
  BOLT_MEM_VLOG(1) << "Prepared async spill for BufferManager block id=" << id_
                   << " size=" << request.memory->Size();
  return request;
}

ByteCount BlockHandle::CommitAsyncSpillSuccess(
    uint64_t expectedSequence,
    SpillLocation location,
    std::unique_ptr<AccountedMemory> memory) {
  std::optional<std::reference_wrapper<ProcessSpillService>> spillToRelease;
  SpillLocation staleLocation;
  ByteCount freed = memory == nullptr ? 0 : memory->Size();
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto context = context_.lock();
    if (state_ != BlockState::kSpilling ||
        evictionSequence_ != expectedSequence || context == nullptr ||
        !context->valid) {
      if (context != nullptr && context->spill.has_value()) {
        spillToRelease = context->spill;
      }
      staleLocation = std::move(location);
      spillScheduled_ = false;
      freed = 0;
    } else {
      spillLocation_ = std::move(location);
      state_ = BlockState::kSpilled;
      spillScheduled_ = false;
    }
  }
  memory.reset();
  if (staleLocation.Valid() && spillToRelease.has_value()) {
    spillToRelease->get().Release(staleLocation);
  }
  cv_.notify_all();
  return freed;
}

void BlockHandle::CommitAsyncSpillFailure(
    uint64_t expectedSequence,
    std::unique_ptr<AccountedMemory> memory) {
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto context = context_.lock();
    if (state_ == BlockState::kSpilling &&
        evictionSequence_ == expectedSequence && context != nullptr &&
        context->valid) {
      memory_ = std::move(memory);
      state_ = BlockState::kLoaded;
      ++evictionSequence_;
    }
    spillScheduled_ = false;
  }
  cv_.notify_all();
}

void BlockHandle::ClearSpillScheduled() noexcept {
  std::lock_guard<std::mutex> l(mutex_);
  spillScheduled_ = false;
}

BlockState BlockHandle::State() const {
  std::lock_guard<std::mutex> l(mutex_);
  return state_;
}

uint64_t BlockHandle::EvictionSequence() const {
  std::lock_guard<std::mutex> l(mutex_);
  return evictionSequence_;
}

ByteCount BlockHandle::Size() const {
  std::lock_guard<std::mutex> l(mutex_);
  return size_;
}

bool BlockHandle::IsSealed() const {
  std::lock_guard<std::mutex> l(mutex_);
  return sealed_;
}

bool BlockHandle::IsPinned() const {
  std::lock_guard<std::mutex> l(mutex_);
  return pinCount_ != 0;
}

void BlockHandle::InstallMemory(std::unique_ptr<AccountedMemory> memory) {
  std::lock_guard<std::mutex> l(mutex_);
  memory_ = std::move(memory);
  state_ = BlockState::kLoaded;
  pinCount_ = 1;
  BOLT_MEM_VLOG(1) << "Installed memory for BufferManager block id=" << id_
                     << " size=" << size_;
}

void BlockHandle::Unpin(bool initialWrite) noexcept {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (pinCount_ > 0) {
      --pinCount_;
    }
    if (initialWrite) {
      sealed_ = true;
    }
    BOLT_MEM_VLOG(1) << "Unpinned BufferManager block id=" << id_
                       << " pinCount=" << pinCount_
                       << " sealed=" << sealed_;
  }
  cv_.notify_all();
}

ConstDataPtr BlockHandle::DataLocked() const {
  BOLT_USER_CHECK(state_ == BlockState::kLoaded, "Block is not loaded");
  BOLT_USER_CHECK_NOT_NULL(memory_, "Block has no resident memory");
  return memory_->Data();
}

DataPtr BlockHandle::MutableDataLocked(bool initialWrite) {
  BOLT_USER_CHECK(
      initialWrite && !sealed_,
      "Only the initial write handle may mutate an unsealed block");
  return const_cast<DataPtr>(DataLocked());
}

} // namespace bytedance::bolt::memory::bm
