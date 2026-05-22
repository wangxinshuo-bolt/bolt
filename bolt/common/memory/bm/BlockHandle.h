/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>

#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/common/memory/bm/BufferManagerConfig.h"
#include "bolt/common/memory/bm/BufferPool.h"
#include "bolt/common/memory/bm/EvictionTypes.h"
#include "bolt/common/memory/bm/SpillCoordinator.h"
#include "bolt/common/memory/bm/SpillFileStore.h"

namespace bytedance::bolt::memory::bm {

class BufferManager;
struct BufferManagerContext;

// Internal state-machine wrapper for a BufferManager block. BlockHandle owns
// the resident memory (or spilled representation) and tracks the lifecycle a
// block goes through: kAllocating -> kLoaded -> kSpilled and back to kLoaded
// on Pin reload. Multiple BufferHandle pins may
// reference the same BlockHandle; the block is evictable iff pinCount_ == 0.
//
// Threading: BlockHandle is fully thread-safe. All public methods take
// mutex_; cv_ is used to coordinate concurrent Pin reloads (only one thread
// performs the I/O, the rest wait on the load generation).
//
// BlockHandle is held inside BufferManager via shared_ptr and inherits
// BlockHandleBase so the BlockEvictor can reference it through weak_ptr in
// EvictionNode.
class BlockHandle : public BlockHandleBase,
                    public std::enable_shared_from_this<BlockHandle> {
 public:
  // Constructs a block descriptor in the kAllocating state. No resident
  // memory is attached yet; BufferManager::Allocate calls InstallMemory()
  // once the BufferPool reservation succeeds. 'manager' must outlive this
  // BlockHandle (BufferManager invalidates blocks on shutdown via
  // InvalidateForManagerDestruction). 'options' is captured by value and
  // drives the eviction policy and recovery callback.
  BlockHandle(
      std::weak_ptr<BufferManagerContext> context,
      AllocateOptions options);

  // Releases any resident memory and spilled file the block still owns.
  // Marks the block invalid. Never throws.
  ~BlockHandle();

  // Returns the current eviction sequence number. Each time the block's
  // identity-as-eviction-candidate changes (e.g. a spill failure restores
  // kLoaded, or a Pin reload bumps the generation) this counter is
  // incremented so that stale EvictionNode entries can be detected.
  uint64_t EvictionSequence() const override;

  // Acquires a read pin on the block, reloading from spilled state if
  // necessary. Returns a non-initial-write BufferHandle on success.
  //
  // Behavior by current State():
  //   kLoaded                -> increments pinCount_, returns immediately.
  //   kSpilled               -> reads back from SpillFileStore via the
  //                              registered recovery path.
  //   kAllocating / kInvalid -> throws BoltUserError.
  //   kEvicting              -> waits on cv_ until eviction resolves, then
  //                              retries (one of the resolutions above).
  //
  // Concurrent Pin calls coordinate via loadGeneration_; only one thread
  // performs the I/O, others wait and re-check. If the load fails the
  // exception is rethrown to all waiters of that generation.
  BufferHandle Pin();

  // Synchronous, cheap-only eviction path used by BlockEvictor for
  // kEvictAndDiscard / kEvictAndRecompute policies. Frees resident memory
  // when safe and returns the bytes actually released (0 if the block was
  // pinned, already evicted, or under a spill policy).
  // Throws nothing under normal operation; bugs in the recovery contract
  // surface as BoltUserError.
  ByteCount TryEvict(ByteCount targetBytes);

  // Writes the immutable block to spill storage and frees resident memory
  // Returns the bytes released (i.e. the resident size that disappeared from
  // the BufferPool). Bumps evictionSequence_. Throws on I/O failure with the
  // block restored to kLoaded.
  ByteCount SpillToDisk();

  // Marks the block kInvalid and releases its accounted memory and spill
  // file. Used by BufferManager during shutdown so dangling BufferHandle
  // pins observe predictable failures rather than a use-after-free.
  void InvalidateForManagerDestruction() noexcept;

  // Marks a spill-policy block as already submitted to async spill.
  // Returns false when the candidate is stale, pinned, already
  // scheduled, or not in a spillable resident state.
  bool TryMarkSpillScheduled(uint64_t expectedSequence);

  // Clears the async-scheduled marker after a service rejection, worker skip,
  // or completed spill attempt.
  void ClearSpillScheduled() noexcept;

  // Owner-thread async spill prepare/commit path. Prepare moves resident
  // memory into a tokenized request keyed by block id; process spill workers
  // write bytes only and never keep this BlockHandle alive. Commit methods
  // are called by the owning BufferManager thread after it drains completions.
  std::optional<SpillCoordinator::SpillRequest> PrepareAsyncSpill(
      uint64_t expectedSequence);
  ByteCount CommitAsyncSpillSuccess(
      uint64_t expectedSequence,
      SpillLocation location,
      std::unique_ptr<AccountedMemory> memory);
  void CommitAsyncSpillFailure(
      uint64_t expectedSequence,
      std::unique_ptr<AccountedMemory> memory);
  std::optional<SpillCoordinator::PrefetchRequest> PrepareAsyncPrefetch();
  ByteCount CommitAsyncPrefetchSuccess(
      uint64_t expectedSequence,
      std::unique_ptr<AccountedMemory> memory);
  void CommitAsyncPrefetchFailure(uint64_t expectedSequence);

  // Returns the current state in the lifecycle state machine.
  BlockState State() const;

  // Returns the logical (uncompressed) block size in bytes. Constant for
  // the lifetime of the block once InstallMemory() has run.
  ByteCount Size() const;

  // Returns true once the initial write window has been closed, i.e. the
  // initial-write BufferHandle has been destroyed. Sealed blocks are
  // immutable and become eligible for eviction.
  bool IsSealed() const;

  // Returns true iff at least one BufferHandle pin is active. A pinned
  // block cannot be evicted, compressed, or spilled.
  bool IsPinned() const;

  // Returns a process-local monotonic block id for diagnostics and log
  // correlation. Stable across the block's lifetime.
  uint64_t Id() const {
    return id_;
  }

 private:
  friend class BufferHandle;
  friend class BufferManager;
  friend class BlockEvictor;

  // Installs newly allocated AccountedMemory and creates the initial-write
  // pin (transitions kAllocating -> kLoaded with pinCount_==1, sealed_==false).
  // Called exactly once by BufferManager::Allocate before the block is
  // exposed to user code.
  void InstallMemory(std::unique_ptr<AccountedMemory> memory);

  // Releases one pin. When 'initialWrite' is true the block is also sealed.
  // Decrements pinCount_; on transition to zero, broadcasts cv_ so
  // eviction/spill paths waiting for unpinning can proceed. Never throws.
  void Unpin(bool initialWrite) noexcept;

  // Returns immutable bytes for the current resident representation.
  // Caller MUST hold mutex_. Throws BoltUserError if the block is not loaded.
  ConstDataPtr DataLocked() const;

  // Returns mutable bytes for the initial-write handle while the block is
  // unsealed. Caller MUST hold mutex_. Throws BoltUserError on a sealed
  // block, on reader handles, or when memory_ has been released.
  DataPtr MutableDataLocked(bool initialWrite);

  const uint64_t id_{0};
  std::weak_ptr<BufferManagerContext> context_;
  const AllocateOptions options_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  BlockState state_{BlockState::kInvalid};
  std::unique_ptr<AccountedMemory> memory_;
  SpillLocation spillLocation_;
  ByteCount size_{0};
  uint32_t pinCount_{0};
  bool sealed_{false};
  // Monotonic load attempt id used to disambiguate concurrent Pin reloads so
  // that one failure is reported exactly once to its waiters.
  uint64_t loadGeneration_{0};
  // Captured exception from the most recent failed reload of loadGeneration_.
  // Cleared whenever a new load attempt succeeds or starts.
  std::exception_ptr lastLoadError_;
  // Incremented whenever the eviction candidate identity changes. Stale
  // EvictionNode entries referencing an older value must be skipped.
  uint64_t evictionSequence_{0};
  bool spillScheduled_{false};
};

} // namespace bytedance::bolt::memory::bm
