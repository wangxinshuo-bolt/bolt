/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bolt/common/memory/bm/Types.h"

namespace bytedance::bolt::memory::bm {

// Single global spill scheduler (design doc §11). Owns the worker pool,
// fairness state, and the global progress epoch. Lives inside the
// ProcessSpillService singleton; do not instantiate directly.
//
// Fairness (DRF-lite): each client carries a virtual-time counter. The
// scheduler always picks the client with the smallest vtime, advances it by
// (bytes / weight) after each task, and re-inserts. This approximates
// weighted fair share without per-resource tracking.
//
// Threading: Submit/WaitForProgress/Register/Unregister are all safe to
// call from any thread. Internal worker threads are owned by the scheduler.
class GlobalSpillScheduler : public SpillRequester {
 public:
  // Constructs the scheduler with 'workerThreadCount' worker threads.
  // workerThreadCount==0 produces a "no-workers" mode where every
  // SubmitSpill returns kBackpressured -- callers fall back to synchronous
  // SpillToDisk. Workers are not started until Start() is called.
  GlobalSpillScheduler(uint32_t workerThreadCount, MetricsRegistry& metrics);

  // Stops worker threads (if started) and drops all queued nodes. Never
  // throws.
  ~GlobalSpillScheduler() override;

  // Starts worker threads. Idempotent: calling twice is a no-op. Must be
  // called before SubmitSpill on configurations with workerThreadCount>0.
  void Start();

  // Signals workers to drain queued work and waits for them to exit.
  // Idempotent. After Stop(), SubmitSpill returns kFailed.
  void Stop();

  // Pushes 'node' onto its client's fairness queue and wakes a worker.
  // The caller MUST set node.clientId to a value previously returned by
  // RegisterClient before calling. Returns:
  //   kScheduled       -- queued.
  //   kBackpressured   -- activeAttempts already at maxActiveAttempts, or
  //                        workerThreadCount==0.
  //   kFailed          -- scheduler is stopping or the client is not
  //                        registered.
  EvictResult SubmitSpill(EvictionNode node) override;

  // Blocks until progressEpoch_ advances (a worker completed a task and
  // freed bytes) or 'timeout' elapses. Returns true on epoch advance,
  // false on timeout or when the scheduler is stopping.
  bool WaitForProgress(
      ByteCount bytesNeeded,
      std::chrono::milliseconds timeout) override;

  // Allocates a fairness slot and returns its id. 'weight' controls vtime
  // accounting: a client with weight 2x another gets ~2x the share. Larger
  // weights drain faster, so the scheduler favors them.
  // The id is unique within the scheduler's lifetime (monotonically
  // increasing).
  uint64_t RegisterClient(uint64_t weight);

  // Drops the client's fairness slot and discards any queued nodes. Safe
  // to call when the client has no pending work; unknown ids are ignored.
  void UnregisterClient(uint64_t clientId);

 private:
  struct ClientFairState {
    uint64_t weight{1};
    double virtualTime{0.0};
    std::deque<EvictionNode> ready;
    bool registered{true};
  };

  // Worker loop drains client deques in vtime order until 'stopping_'.
  // Each iteration: PickNext, run the spill, charge vtime, notify progress.
  void WorkerLoop();

  // Wakes any thread waiting on WaitForProgress. Holds nothing.
  void NotifyProgress();

  // Picks the client with smallest vtime that has a runnable node.
  // On success, removes the node from the client's queue and returns true.
  // Returns false when no client has queued work. Caller must hold mutex_.
  bool PickNextLocked(EvictionNode& out, uint64_t& ownerId);

  // Advances a client's vtime by bytes / weight. Caller holds mutex_.
  // Unknown client ids are ignored (the client may have unregistered
  // between PickNextLocked and ChargeClientLocked).
  void ChargeClientLocked(uint64_t clientId, ByteCount bytes);

  MetricsRegistry& metrics_;
  const uint32_t workerThreadCount_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::condition_variable progressCv_;
  bool stopping_{false};
  bool started_{false};
  uint64_t progressEpoch_{0};
  std::atomic<uint64_t> nextClientId_{1};
  std::unordered_map<uint64_t, ClientFairState> clients_;
  uint32_t activeAttempts_{0};
  std::vector<std::thread> workers_;
};

} // namespace bytedance::bolt::memory::bm
