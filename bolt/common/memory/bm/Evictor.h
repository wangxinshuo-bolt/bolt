/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <functional>

#include "bolt/common/memory/bm/EvictionTypes.h"

namespace bytedance::bolt::memory::bm {

class BlockHandle;
class BufferManager;

// Eviction queue interface. BufferManager's reclaimer pops candidates from
// this object and feeds them through one of
// the Try*Evict variants to convert pinned-but-evictable blocks into freed
// bytes.
//
// The contract splits sync and async paths to keep the locking simple:
//   * TryEvictNodeSync handles cheap policies (kFreeOrCheap).
//     Returns kFreed/kSkipped/kFailed only.
//   * TryScheduleEvict hands kSpill nodes off to the spill service.
//     Returns kScheduled/kBackpressured/kSkipped/kFailed only.
//   * WaitForProgress blocks until the spill subsystem makes forward
//     progress (kFreed bytes) or the timeout elapses.
//
// All implementations must be thread-safe; multiple BufferPool slow paths
// may invoke them concurrently.
class Evictor {
 public:
  virtual ~Evictor() = default;

  // Enqueues a candidate at the back of its (cost, priority) bucket.
  // Implementations MUST tolerate stale nodes -- the corresponding block
  // may have been pinned, evicted, or destroyed since the node was built;
  // such nodes simply return kSkipped from Try*Evict.
  // Out-of-range cost/priority values are silently dropped.
  virtual void Enqueue(EvictionNode node) = 0;

  // Synchronously evicts a cheap candidate by calling BlockHandle::TryEvict.
  // Never performs I/O. Returns:
  //   kFreed{N}  -- freed N bytes; the block is now in a non-resident
  //                  state.
  //   kSkipped   -- node was stale, the policy is kSpillToDisk, or the
  //                  block is already in the target state. No bytes freed.
  //   kFailed    -- the call would have produced bytes but the block
  //                  reported a non-zero error path (currently unused).
  virtual EvictResult TryEvictNodeSync(const EvictionNode& node) = 0;

  // Submits a kSpill candidate to the registered SpillRequester. Returns:
  //   kScheduled       -- request accepted; bytes will be reclaimed
  //                        asynchronously, callers should WaitForProgress.
  //   kBackpressured   -- spill subsystem is saturated; caller should back
  //                        off rather than retry immediately.
  //   kSkipped         -- node is stale or its policy is not a spill policy.
  //   kFailed          -- no spill requester is wired (e.g. shutting down).
  virtual EvictResult TryScheduleEvict(const EvictionNode& node) = 0;

  // Pops the cheapest, lowest-priority candidate. Returns false (and leaves
  // 'out' untouched) when no node is queued. The popped node is removed
  // from the queue regardless of whether the caller can act on it; if the
  // caller decides not to evict it, it must Enqueue() it again.
  virtual bool TryPopAnyCandidate(EvictionNode& out) = 0;

  // Blocks the calling thread until either (a) the spill subsystem reports
  // at least one kFreed result that may have helped reach 'bytesNeeded',
  // or (b) 'timeout' elapses, or (c) the SpillRequester is detached.
  // Returns true when progress was observed, false otherwise. Callers must
  // re-check their reservation after this returns regardless of the result.
  virtual bool WaitForProgress(
      ByteCount bytesNeeded,
      std::chrono::milliseconds timeout) = 0;
};

// Concrete Evictor backed by per-cost / per-priority FIFO queues. Sync
// eviction is fully handled in-process; spill goes through the process spill
// service via the SpillRequester interface.
class BlockEvictor : public Evictor {
 public:
  // Builds an evictor without a spill requester. Call SetSpillRequester
  // before the first TryScheduleEvict / WaitForProgress invocation
  // 'manager' must outlive this evictor; only used for diagnostic context,
  // no callbacks are stored.
  explicit BlockEvictor(BufferManager& manager);

  ~BlockEvictor() override = default;

  // Installs or detaches the SpillRequester used by TryScheduleEvict and
  // WaitForProgress. The requester is a non-owning reference to a
  // process-owned spill service.
  void SetSpillRequester(SpillRequester& requester);
  void ClearSpillRequester();

  // See Evictor::Enqueue. Implementation appends to queues_[cost][priority]
  // under mutex_. Out-of-range cost/priority values are dropped silently.
  void Enqueue(EvictionNode node) override;

  // See Evictor::TryEvictNodeSync. Validates the node, dispatches on
  // EvictPolicy, and asks the BlockHandle to do the actual work.
  EvictResult TryEvictNodeSync(const EvictionNode& node) override;

  // See Evictor::TryScheduleEvict. Validates the node, then forwards to
  // SpillRequester::SubmitSpill on the registered requester.
  EvictResult TryScheduleEvict(const EvictionNode& node) override;

  // See Evictor::TryPopAnyCandidate. Iterates queues in cost-then-priority
  // order (kFreeOrCheap < kSpill, kLow < kCritical) and pops the first
  // non-empty bucket's front.
  bool TryPopAnyCandidate(EvictionNode& out) override;

  // See Evictor::WaitForProgress. Forwards to the registered requester;
  // returns false immediately when no requester is wired.
  bool WaitForProgress(
      ByteCount bytesNeeded,
      std::chrono::milliseconds timeout) override;

 private:
  static constexpr size_t kCostClassCount = 2;
  static constexpr size_t kPriorityCount = 4;

  // Validates a node against its block and returns the locked block if it
  // remains a valid victim. Empty means kSkipped: destroyed block, wrong
  // concrete type, sequence mismatch, or currently pinned.
  std::shared_ptr<BlockHandle> ValidateNode(const EvictionNode& node) const;

  BufferManager& manager_;
  std::optional<std::reference_wrapper<SpillRequester>> spillRequester_;
  mutable std::mutex mutex_;
  // FIFO buckets indexed by eviction cost and priority.
  std::deque<EvictionNode> queues_[kCostClassCount][kPriorityCount];
};

} // namespace bytedance::bolt::memory::bm
