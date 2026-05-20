/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/BlockHandle.h"
#include "bolt/common/memory/bm/BufferManagerConfig.h"
#include "bolt/common/memory/bm/BufferPool.h"
#include "bolt/common/memory/bm/Evictor.h"
#include "bolt/common/memory/bm/ProcessSpillService.h"

namespace bytedance::bolt::memory {
class MemoryManager;
class MemoryReclaimer;
} // namespace bytedance::bolt::memory

namespace bytedance::bolt::memory::bm {

// Top-level entry point for BufferManager. It owns:
//   * a dedicated subtree of Bolt's MemoryPool for physical bytes
//   * a BufferPool that tracks logical usage
//   * a BufferAllocator pairing the two
//   * a BlockEvictor wired to the process-wide spill service when needed
//
// All lookups (Allocate / Pin / Reclaim / Snapshot) are thread-safe.
// BufferManager must outlive every BufferHandle and BlockHandle it produces;
// the destructor invalidates outstanding blocks first to keep dangling pins
// from triggering use-after-free.
class BufferManager {
 public:
  // Creates a BufferManager backed by a dedicated Bolt MemoryPool subtree.
  // The constructor registers the pool subtree under 'memoryManager'.
  // Throws BoltUserError on invalid config (see BufferManagerConfig docs).
  BufferManager(MemoryManager& memoryManager, BufferManagerConfig config);

  // Stops background work, invalidates live blocks, and tears down owned
  // pools. After the destructor returns, any still-live
  // BufferHandle observes an invalidated block (operations throw rather than
  // crash). Never throws.
  ~BufferManager();

  // Allocates a new mutable block of size options.size and returns the
  // initial-write pin. The caller must populate the bytes through
  // BufferHandle::MutableData and then drop the handle to seal the block.
  // Throws BoltUserError on out-of-range tag or bytes.
  BufferHandle Allocate(AllocateOptions options);

  // Allocates a block, runs 'init(data, size)' under the initial write pin,
  // and seals the block before returning. The returned shared_ptr owns the
  // block in its post-write, unpinned state -- callers must Pin() to read
  // it back. Useful for one-shot block production.
  // 'init' must not throw; if it throws, the partial allocation is rolled
  // back and the exception propagates.
  std::shared_ptr<BlockHandle> AllocatePersistent(
      AllocateOptions options,
      std::function<void(DataPtr, ByteCount)> init);

  // Pins an existing block, reloading it from compressed/spilled state if
  // necessary. Returns a non-initial-write BufferHandle (read-only). See
  // BlockHandle::Pin for the full state-by-state contract.
  BufferHandle Pin(const std::shared_ptr<BlockHandle>& block);

  // Allocates raw accounted memory for callers that do not need block-handle
  // semantics (e.g. operator scratch). Records usage under (tag, kind),
  // allocates physical bytes from the BufferManager's MemoryPool, and
  // returns an RAII wrapper that releases both on destruction.
  std::unique_ptr<AccountedMemory> AllocateMemory(
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind = ReservationKind::kNormal);

  // Reserves logical BufferPool usage without allocating physical bytes.
  // Useful when callers want to reserve up front, then materialize bytes
  // later through Bolt's MemoryPool.
  BufferPoolReservation ReserveMemory(
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind = ReservationKind::kNormal);

  // Returns total logical BufferPool usage across all tags. Equivalent to
  // pool().GetMemoryUsage().
  ByteCount GetMemoryUsage() const;

  // Returns logical BufferPool usage for a single memory tag. Throws
  // BoltUserError if 'tag' is out of range.
  ByteCount GetMemoryUsage(MemoryTag tag) const;

  // Captures a consistent snapshot of all BufferPool counters. See
  // BufferPoolSnapshot for the field-level invariants.
  BufferPoolSnapshot Snapshot() const;

  // Reclaims up to 'targetBytes' from the eviction queue, dispatching nodes
  // through the BlockEvictor. targetBytes==0 means "best-effort, until the
  // queue is empty". Returns the bytes actually freed (may be 0). Spill
  // requests are submitted asynchronously; this call waits for them to
  // make progress before returning.
  ByteCount Reclaim(ByteCount targetBytes);

  // Returns currently resident, unpinned bytes that can participate in
  // reclaim. Useful for back-pressure heuristics; not a hard limit.
  ByteCount ReclaimableBytes() const;

  // Exposes the internal allocator to BlockHandle reload paths. Lifetime
  // is tied to this BufferManager.
  BufferAllocator& Allocator() {
    return allocator_;
  }

  // Exposes the process-wide spill service used by BlockHandle for spill and
  // reload I/O. Returns nullptr until a spill policy allocation wires it.
  ProcessSpillService* Spill() const {
    return spillService_;
  }

  // Exposes the eviction queue for diagnostics and tests. Production code
  // should not enqueue directly -- BufferManager does so
  // automatically when a block becomes evictable.
  Evictor& EvictionQueue() {
    return evictor_;
  }

 private:
  // Tracks a weak reference so reclaim can find live blocks without owning
  // them. Called by Allocate / AllocatePersistent right after a block has
  // been constructed.
  void RegisterBlock(const std::shared_ptr<BlockHandle>& block);

  // Pushes an EvictionNode for 'block' if its policy participates in
  // eviction. Idempotent: stale nodes are tolerated by the queue. Called
  // whenever a block transitions to an evictable state (sealed, unpinned,
  // kLoaded).
  void EnqueueEvictionCandidate(const std::shared_ptr<BlockHandle>& block);

  // Builds a fresh EvictionNode snapshot for 'block'. BlockHandle
  // internals are private, so this lives on BufferManager (a BlockHandle
  // friend) rather than on BlockHandle itself.
  static EvictionNode MakeEvictionNode(
      const std::shared_ptr<BlockHandle>& block);

  // Builds the MemoryPool reclaimer used by Bolt arbitration. The reclaimer
  // delegates to BufferManager::Reclaim and is registered on rootPool_
  // during construction.
  std::unique_ptr<MemoryReclaimer> CreateReclaimer();

  void EnsureSpillService();

  MemoryManager& memoryManager_;
  BufferManagerConfig config_;
  MetricsRegistry& metrics_;
  Counter& allocateRequestsCounter_;
  Counter& reclaimRequestsCounter_;
  Counter& reclaimBytesCounter_;
  Gauge& usedMemoryGauge_;
  Gauge& pinnedMemoryGauge_;
  Histogram& allocateDuration_;
  Histogram& reclaimDuration_;
  std::shared_ptr<MemoryPool> rootPool_;
  std::shared_ptr<MemoryPool> leafPool_;
  BufferPool pool_;
  BufferAllocator allocator_;
  ProcessSpillService* spillService_{nullptr};
  BlockEvictor evictor_;
  mutable std::mutex mutex_;
  bool shuttingDown_{false};
  std::vector<std::weak_ptr<BlockHandle>> blocks_;
};

} // namespace bytedance::bolt::memory::bm
