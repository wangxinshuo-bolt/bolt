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
// before heavy spill writes (per design doc §7.2). Order is enforced as
//     kFreeOrCheap < kCompress < kSpill
// and is the SAME order produced by EvictionCostFor().
enum class EvictionCostClass : uint8_t {
  kFreeOrCheap,
  kCompress,
  kSpill,
};

// Lifecycle states of a BlockHandle. Allowed transitions:
//   kInvalid        -> kLoaded   (only via InstallMemory after Allocate)
//   kLoaded         -> kSpilling | kCompressed | kDiscarded |
//                      kEvictedRecomputable
//   kSpilling       -> kSpilled (success) | kLoaded/kCompressed (failure)
//   kCompressed     -> kSpilling | kLoading
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
  kCompressed,
  kSpilled,
  kDiscarded,
  kEvictedRecomputable,
};

// Outcome of a single eviction attempt as defined in design doc §4.
//   kFreed         memory was actually released back to BufferPool; freedBytes
//                  is the number of bytes the caller may treat as reclaimed.
//   kScheduled     spill submitted to the global scheduler; freedBytes is 0
//                  because the bytes are not yet released.
//   kBackpressured scheduler accepted the candidate but could not start I/O
//                  (e.g. workerThreadCount == 0); Reserve must wait on
//                  WaitForProgress and try again.
//   kSkipped       candidate is stale (block expired, evictionSequence moved
//                  on, block currently pinned, or policy mismatches the path)
//                  – the caller should drop the node and pop the next one.
//   kFailed        evict could not run due to shutdown, missing requester,
//                  or fatal scheduler state. Always paired with freedBytes==0.
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
  // to kLoaded/kCompressed). The Evictor compares it against
  // EvictionNode::evictionSequence and treats a mismatch as a stale node.
  virtual uint64_t EvictionSequence() const = 0;
};

// Append-only candidate placed on the eviction queue (design doc §7.1).
// Stale nodes are tolerated: the scheduler re-validates evictionSequence
// before acting on a node, so a slot may safely be re-enqueued without
// purging earlier entries. Callers are expected to populate every field
// before Enqueue:
//   block            – weak ref to the candidate block
//   evictionSequence – snapshot of block->EvictionSequence() at enqueue time
//   cost             – EvictionCostFor(policy, state) at enqueue time
//   priority         – the block's AllocateOptions::priority
//   enqueueTimeMs    – best-effort wall-clock millis (may stay 0 in tests)
//   clientId         – set automatically by SpillClient::SubmitSpill
struct EvictionNode {
  std::weak_ptr<BlockHandleBase> block;
  uint64_t evictionSequence{0};
  EvictionCostClass cost{EvictionCostClass::kSpill};
  Priority priority{Priority::kNormal};
  int64_t enqueueTimeMs{0};
  // Routing key into ProcessSpillService's GlobalSpillScheduler. SpillClient
  // stamps this before forwarding a node so the scheduler can charge the
  // right tenant fairness slot. Zero means "untagged" (only used during
  // intermediate routing inside BlockEvictor).
  uint64_t clientId{0};
};

// Abstract contract through which BlockEvictor pushes spill candidates and
// blocks on backpressure (design doc §11.2). SpillClient is the production
// implementation but tests may inject fakes to drive deterministic scenarios.
class SpillRequester {
 public:
  virtual ~SpillRequester() = default;
  // Hands an evict candidate to the scheduler. Return contract:
  //   kScheduled     – node accepted, will run asynchronously.
  //   kBackpressured – node accepted but parked (e.g. no workers). Reserve
  //                    must NOT re-enqueue it.
  //   kFailed        – scheduler is stopping or the client/service is gone.
  //   kSkipped       – the weak_ptr is already expired.
  // Implementations must never silently drop a node – Reserve's slow path
  // would spin-wait forever.
  virtual EvictResult SubmitSpill(EvictionNode node) = 0;
  // Blocks until the scheduler observes progress (spill success/failure,
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
// "loading", "compressed", "spilled", "discarded", "evicted_recomputable").
// Never throws.
const char* ToString(BlockState state);
// Returns a stable lower-case debug string for 'cost' (e.g. "free_or_cheap",
// "compress", "spill"). Never throws.
const char* ToString(EvictionCostClass cost);
// Returns a stable lower-case debug string for 'kind' matching the enum
// names (e.g. "freed", "scheduled", "backpressured", "skipped", "failed").
// Never throws.
const char* ToString(EvictResultKind kind);

// Pure mapping from (policy, state) to the cost-class bucket the eviction
// queue stores it in:
//   kDiscard / kRecompute               -> kFreeOrCheap
//   kCompressThenSpill in kCompressed   -> kSpill (only spill remains)
//   kCompressThenSpill in any other     -> kCompress
//   kSpillToDisk                        -> kSpill
//   kPinnedForever                      -> kSpill (never enqueued in
//                                                  practice; defensive)
EvictionCostClass EvictionCostFor(EvictPolicy policy, BlockState state);

} // namespace bytedance::bolt::memory::bm
