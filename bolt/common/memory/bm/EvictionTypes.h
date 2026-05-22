/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "bolt/common/memory/bm/MemoryTypes.h"

namespace bytedance::bolt::memory::bm {

// Coarse cost ranking used by the eviction queue. The Reserve slow path
// scans cheap classes first so that low-cost candidates are sacrificed
// before heavy spill writes. Order is enforced as
//     kFreeOrCheap < kSpill
// and is the SAME order produced by EvictionCostFor().
enum class EvictionCostClass : uint8_t {
  kFreeOrCheap,
  kSpill,
};

// Lifecycle states of a BlockHandle. Allowed transitions:
//   kInvalid        -> kLoaded   (only via InstallMemory after Allocate)
//   kLoaded         -> kSpilling | kDiscarded | kEvictedRecomputable
//   kSpilling       -> kSpilled (success) | kLoaded (failure)
//   kSpilled        -> kLoading
//   kEvictedRecomputable -> kLoading
//   kLoading        -> kLoaded (success) | previous resting state (failure)
//   any             -> kInvalid (BufferManager destruction)
// Pin() observers see kInvalid / kDiscarded as "no longer valid" and return
// an empty BufferHandle.
enum class BlockState : uint8_t {
  kInvalid,
  kLoaded,
  kLoading,
  kSpilling,
  kSpilled,
  kDiscarded,
  kEvictedRecomputable,
};

// Outcome of a single eviction attempt.
//   kFreed         memory was actually released back to BufferPool; freedBytes
//                  is the number of bytes the caller may treat as reclaimed.
//   kScheduled     spill submitted to the spill coordinator; freedBytes is 0
//                  because the bytes are not yet released.
//   kBackpressured spill coordinator could not accept more async I/O now.
//   kSkipped       candidate is stale (block expired, evictionSequence moved
//                  on, block currently pinned, or policy mismatches the path)
//                  – the caller should drop the node and pop the next one.
//   kFailed        evict could not run due to shutdown, missing requester,
//                  or fatal spill coordinator state. Always paired with
//                  freedBytes==0.
enum class EvictResultKind : uint8_t {
  kFreed,
  kScheduled,
  kBackpressured,
  kSkipped,
  kFailed,
};

// Pair returned from every eviction attempt. freedBytes is meaningful only
// when kind == kFreed (it is the post-evict delta in BufferPool usage); all
// other kinds report 0.
struct EvictResult {
  EvictResultKind kind{EvictResultKind::kSkipped};
  ByteCount freedBytes{0};
};

// Internal type-erased view of BlockHandle used by the eviction queue.
// Concrete BlockHandle is the only production implementation; tests may
// implement this interface directly to drive Evictor scenarios.
class BlockHandleBase {
 public:
  virtual ~BlockHandleBase() = default;
  // Monotonic counter incremented by BlockHandle whenever the eviction
  // candidate identity changes (e.g. a failed spill that restored the block
  // to kLoaded). The Evictor compares it against
  // EvictionNode::evictionSequence and treats a mismatch as a stale node.
  virtual uint64_t EvictionSequence() const = 0;
};

// Append-only candidate placed on the eviction queue. Stale nodes are
// tolerated: async spill re-validates evictionSequence before acting on a
// node, so a slot may safely be re-enqueued without purging earlier entries.
// Callers are expected to populate every field before Enqueue:
//   block            – weak ref to the candidate block
//   evictionSequence – snapshot of block->EvictionSequence() at enqueue time
//   cost             – EvictionCostFor(policy, state) at enqueue time
//   priority         – the block's AllocateOptions::priority
//   enqueueTimeMs    – best-effort wall-clock millis (may stay 0 in tests)
struct EvictionNode {
  std::weak_ptr<BlockHandleBase> block;
  uint64_t evictionSequence{0};
  EvictionCostClass cost{EvictionCostClass::kSpill};
  Priority priority{Priority::kNormal};
  int64_t enqueueTimeMs{0};
};

// Abstract contract through which BlockEvictor pushes spill candidates and
// blocks on backpressure. SpillCoordinator is the production
// implementation; tests may inject fakes to drive deterministic scenarios.
class SpillRequester {
 public:
  virtual ~SpillRequester() = default;
  // Hands an evict candidate to the spill coordinator. Return contract:
  //   kScheduled     – node accepted, will run asynchronously.
  //   kBackpressured – async spill is temporarily unavailable.
  //   kFailed        – spill coordinator is stopping or gone.
  //   kSkipped       – the weak_ptr is already expired.
  // Implementations must never silently drop a node – Reserve's slow path
  // would spin-wait forever.
  virtual EvictResult SubmitSpill(EvictionNode node) = 0;
  // Blocks until the spill coordinator observes progress (spill success/failure,
  // inflight bytes drop, token release, shutdown) or 'timeout' elapses.
  // 'bytesNeeded' is advisory – some implementations may return early when
  // enough memory has been freed; the default contract returns true on any
  // observed progress epoch advance and false on timeout/shutdown without
  // progress.
  virtual bool WaitForProgress(
      ByteCount bytesNeeded,
      std::chrono::milliseconds timeout) = 0;
};

// Returns a stable lower-case debug string for 'state' (e.g. "loaded",
// "loading", "spilled", "discarded", "evicted_recomputable").
// Never throws.
const char* ToString(BlockState state);
// Returns a stable lower-case debug string for 'cost' (e.g. "free_or_cheap",
// "spill"). Never throws.
const char* ToString(EvictionCostClass cost);
// Returns a stable lower-case debug string for 'kind' matching the enum
// names (e.g. "freed", "scheduled", "backpressured", "skipped", "failed").
// Never throws.
const char* ToString(EvictResultKind kind);

// Pure mapping from (policy, state) to the cost-class bucket the eviction
// queue stores it in:
//   kDiscard / kRecompute               -> kFreeOrCheap
//   kSpillToDisk                        -> kSpill
//   kPinnedForever                      -> kSpill (never enqueued in
//                                                  practice; defensive)
EvictionCostClass EvictionCostFor(EvictPolicy policy, BlockState state);

} // namespace bytedance::bolt::memory::bm
