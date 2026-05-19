/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/BlockHandle.h"

#include <algorithm>
#include <atomic>
#include <cstring>
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

} // namespace

BlockHandle::BlockHandle(BufferManager* manager, AllocateOptions options)
    : id_(nextBlockId()),
      manager_(manager),
      options_(std::move(options)),
      size_(options_.size) {
  BOLT_MEM_LOG(INFO) << "Created BufferManager block id=" << id_
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
  std::unique_ptr<AccountedMemory> oldCompressed;
  std::shared_ptr<SpillClient> spillForRelease;
  std::optional<SpillLocation> spillToRelease;
  uint64_t myGeneration = 0;
  {
    std::unique_lock<std::mutex> l(mutex_);
    for (;;) {
      if (state_ == BlockState::kLoaded) {
        ++pinCount_;
        BOLT_MEM_LOG(INFO) << "Pinned resident BufferManager block id=" << id_
                           << " pinCount=" << pinCount_;
        return BufferHandle(shared_from_this(), false);
      }
      if (state_ == BlockState::kDiscarded || state_ == BlockState::kInvalid) {
        BOLT_MEM_LOG(INFO) << "Pin returned invalid for BufferManager block id="
                           << id_ << " state=" << ToString(state_);
        return BufferHandle();
      }
      if (state_ == BlockState::kSpilling || state_ == BlockState::kLoading) {
        // Snapshot the generation we are observing so that, if loading fails,
        // we propagate the same error to all waiters instead of each thread
        // spinning back into Loading themselves (per design doc §8.6).
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
            state_ == BlockState::kSpilled ||
            state_ == BlockState::kCompressed,
        "Unsupported BufferManager block state");
    if (state_ == BlockState::kSpilled) {
      oldSpill = spillLocation_;
    } else if (state_ == BlockState::kCompressed) {
      // Hold a local handle so decompress can run without the lock; the actual
      // release happens after we install the new memory.
      oldCompressed = std::move(compressed_);
    }
    BOLT_MEM_LOG(INFO) << "Loading BufferManager block id=" << id_
                       << " from state=" << ToString(state_);
    state_ = BlockState::kLoading;
    myGeneration = ++loadGeneration_;
    lastLoadError_ = nullptr;
  }

  try {
    loaded = manager_->Allocator().Allocate(
        options_.tag, size_, BodyReservationKind(options_.policy));
    if (oldSpill.has_value()) {
      manager_->Spill()->Read(*oldSpill, loaded->Data(), loaded->Size());
    } else if (oldCompressed != nullptr) {
      // MVP: compressed_ is a passthrough copy of the original block bytes.
      BOLT_USER_CHECK_EQ(
          oldCompressed->Size(),
          loaded->Size(),
          "Compressed payload size mismatch");
      std::memcpy(loaded->Data(), oldCompressed->Data(), loaded->Size());
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
        } else if (oldCompressed != nullptr) {
          state_ = BlockState::kCompressed;
          // Restore ownership so future Pin attempts can retry.
          compressed_ = std::move(oldCompressed);
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
    if (oldSpill.has_value() && manager_ != nullptr) {
      spillForRelease = manager_->Spill();
      spillToRelease = *oldSpill;
      spillLocation_ = SpillLocation{};
    }
    oldCompressed.reset();
    ++pinCount_;
    BOLT_MEM_LOG(INFO) << "Loaded and pinned BufferManager block id=" << id_
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
  BOLT_MEM_LOG(INFO) << "Evicted BufferManager block id=" << id_
                     << " policy=" << ToString(options_.policy)
                     << " freed=" << freed;
  return std::min(freed, targetBytes == 0 ? freed : targetBytes);
}

ByteCount BlockHandle::CompressInPlace() {
  // Per design doc §8.4 / §9.2: kCompressThenSpill compresses the resident
  // block into compressed_, releases the original BlockBuffer, and leaves the
  // block ready to be either spilled later or pinned through decompress.
  std::unique_ptr<AccountedMemory> evicted;
  std::unique_ptr<AccountedMemory> compressed;
  std::vector<uint8_t> copy;
  ByteCount originalBytes = 0;
  ByteCount compressedBytes = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (options_.policy != EvictPolicy::kCompressThenSpill ||
        state_ != BlockState::kLoaded || pinCount_ != 0 || memory_ == nullptr ||
        manager_ == nullptr) {
      return 0;
    }
    originalBytes = memory_->Size();
    // Copy while holding the block mutex so manager destruction or a racing
    // eviction cannot free memory_ underneath us. The quota-bearing allocation
    // happens after dropping the mutex to avoid Reserve()->Reclaim() trying to
    // re-enter this block and deadlocking on mutex_. kSpilling is used here as
    // a transient "externalization in progress" state; Pin waits on it and
    // reclaim skips it just like a real spill attempt.
    copy.resize(originalBytes);
    std::memcpy(copy.data(), memory_->Data(), originalBytes);
    state_ = BlockState::kSpilling;
  }

  try {
    // MVP compressor is a passthrough: allocate a separate AccountedMemory
    // chunk and copy the bytes. This keeps compressed_ as the sole owner per
    // design doc §5.1. If this allocation cannot be satisfied under pressure,
    // restore kLoaded and let Reclaim fall through to direct spill.
    compressed = manager_->Allocator().Allocate(
        options_.tag, originalBytes, ReservationKind::kNormal);
    std::memcpy(compressed->Data(), copy.data(), originalBytes);
    compressedBytes = compressed->Size();
  } catch (...) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (state_ == BlockState::kSpilling) {
        state_ = BlockState::kLoaded;
        ++evictionSequence_;
      }
    }
    cv_.notify_all();
    BOLT_MEM_LOG(WARNING)
        << "Failed to compress BufferManager block id=" << id_
        << ", restored to loaded for direct spill fallback";
    return 0;
  }

  {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ != BlockState::kSpilling || manager_ == nullptr) {
      cv_.notify_all();
      return 0;
    }
    compressed_ = std::move(compressed);
    evicted = std::move(memory_);
    state_ = BlockState::kCompressed;
  }
  evicted.reset();
  cv_.notify_all();
  // Net freed bytes = original size - compressed size. With the MVP
  // passthrough compressor this is 0.
  const auto freed =
      originalBytes > compressedBytes ? originalBytes - compressedBytes : 0;
  BOLT_MEM_LOG(INFO) << "Compressed BufferManager block id=" << id_
                     << " original=" << originalBytes
                     << " compressed=" << compressedBytes
                     << " freed=" << freed;
  return freed;
}

ByteCount BlockHandle::SpillToDisk() {
  std::vector<uint8_t> copy;
  std::shared_ptr<SpillClient> spill;
  bool fromCompressed = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!IsSpillPolicy(options_.policy) ||
        (state_ != BlockState::kLoaded && state_ != BlockState::kCompressed) ||
        pinCount_ != 0 || manager_ == nullptr) {
      return 0;
    }
    if (state_ == BlockState::kLoaded && memory_ == nullptr) {
      return 0;
    }
    if (state_ == BlockState::kCompressed && compressed_ == nullptr) {
      return 0;
    }
    // kSpilling deliberately keeps memory_/compressed_ resident. Only a
    // committed write may release the AccountedMemory; otherwise Reserve
    // could observe phantom freed bytes while the only copy is not durable.
    fromCompressed = state_ == BlockState::kCompressed;
    state_ = BlockState::kSpilling;
    if (fromCompressed) {
      copy.resize(compressed_->Size());
      std::memcpy(copy.data(), compressed_->Data(), compressed_->Size());
    } else {
      copy.resize(memory_->Size());
      std::memcpy(copy.data(), memory_->Data(), memory_->Size());
    }
    spill = manager_->Spill();
    BOLT_MEM_LOG(INFO) << "Spilling BufferManager block id=" << id_
                       << " size=" << copy.size()
                       << " policy=" << ToString(options_.policy)
                       << " fromCompressed=" << fromCompressed;
  }

  SpillLocation location;
  try {
    location = spill->Write(options_.tag, copy.data(), copy.size());
  } catch (...) {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ == BlockState::kSpilling) {
      state_ =
          fromCompressed ? BlockState::kCompressed : BlockState::kLoaded;
      // Spill failure changes candidate identity: any in-flight EvictionNode
      // that points at this block becomes stale and must be skipped before
      // a fresh node is enqueued (design doc §8.5, §9.2).
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
    if (state_ != BlockState::kSpilling || manager_ == nullptr) {
      // BufferManager destruction (or a racing invalidate) raced ahead of
      // this committed write. Drop the spill file we just produced so it
      // does not leak.
      spill->Release(location);
      cv_.notify_all();
      return 0;
    }
    spillLocation_ = std::move(location);
    if (fromCompressed) {
      evicted = std::move(compressed_);
    } else {
      evicted = std::move(memory_);
    }
    state_ = BlockState::kSpilled;
  }
  const auto freed = evicted == nullptr ? 0 : evicted->Size();
  evicted.reset();
  cv_.notify_all();
  BOLT_MEM_LOG(INFO) << "Spilled BufferManager block id=" << id_
                     << " freed=" << freed
                     << " file=" << spillLocation_.path;
  return freed;
}

void BlockHandle::InvalidateForManagerDestruction() noexcept {
  std::unique_ptr<AccountedMemory> old;
  std::unique_ptr<AccountedMemory> oldCompressed;
  std::shared_ptr<SpillClient> spill;
  SpillLocation locationToRelease;
  {
    std::lock_guard<std::mutex> l(mutex_);
    state_ = BlockState::kInvalid;
    old = std::move(memory_);
    oldCompressed = std::move(compressed_);
    if (manager_ != nullptr) {
      spill = manager_->Spill();
      if (spillLocation_.Valid() && spill != nullptr) {
        locationToRelease = std::move(spillLocation_);
        spillLocation_ = SpillLocation{};
      }
    }
    manager_ = nullptr;
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
  if (locationToRelease.Valid() && spill != nullptr) {
    spill->Release(locationToRelease);
  }
  old.reset();
  oldCompressed.reset();
  cv_.notify_all();
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
  BOLT_MEM_LOG(INFO) << "Installed memory for BufferManager block id=" << id_
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
    BOLT_MEM_LOG(INFO) << "Unpinned BufferManager block id=" << id_
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
