/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "bolt/common/memory/bm/BufferPool.h"

namespace bytedance::bolt::memory::bm {

// Caller-supplied parameters used at registration. Fields drive the advisory
// math performed by TemporaryMemoryManager::ComputeDecision; the manager
// never allocates anything based on them.
struct TempMemoryRegisterOptions {
  // Identifiers used only for logging/metrics; not consulted by the math.
  std::string queryId;
  std::string operatorId;
  std::string operatorName;
  // Tag the operator would charge under if it called BufferPool::Reserve.
  // Used by future implementations to specialize fairness; ignored today.
  MemoryTag tag{MemoryTag::kOperatorState};
  // Operator-supplied estimate of the bytes still required to finish.
  // Larger values bias the manager toward granting more headroom; smaller
  // values release headroom for siblings. May be 0 if unknown.
  ByteCount estimatedRemainingBytes{0};
  // Floor below which the operator should externalize (spill) instead of
  // expanding in memory. Drives ReservationDecision::shouldExternalize.
  ByteCount minimumReservationBytes{0};
  // Reserved for future weighting; the MVP ignores this field.
  double spillPenalty{1.0};
};

// Output produced by TemporaryMemoryManager. Purely advisory: the operator
// has not been charged anything yet. Fields are mutually compatible -- e.g.
// shouldWait is only meaningful when shouldExternalize is false.
struct ReservationDecision {
  // Recommended reservation size. Always <= estimatedRemainingBytes and
  // <= the per-operator fair share derived from BufferPoolSnapshot.
  ByteCount reservation{0};
  // True if the recommendation falls below minimumReservationBytes. The
  // operator should externalize/spill instead of attempting to expand.
  bool shouldExternalize{false};
  // True if the manager is starved (availableForOperators == 0) but the
  // recommendation is at least minimumReservationBytes. The operator may
  // wait for memory to free up before retrying.
  bool shouldWait{false};
};

class TemporaryMemoryManager;

// Per-operator handle. Construct via TemporaryMemoryManager::Register; the
// destructor unregisters automatically. All accessors are thread-safe.
class TemporaryMemoryState {
 public:
  // Unregisters this advisory state from its owning manager. Safe to call
  // even after InvalidateForManagerDestruction. Never throws.
  ~TemporaryMemoryState();

  // Returns the latest recommended temporary memory reservation in bytes.
  // Returns 0 after the manager has been destroyed (invalidated state).
  ByteCount Reservation() const;

  // Returns the full advisory decision currently associated with this
  // operator. After invalidation returns {0, true, false} so the operator
  // sees "should externalize" and gracefully tears down.
  ReservationDecision Decision() const;

  // Updates the operator's remaining-work estimate and asks the manager to
  // recompute every live operator's budget. Cheap on the caller (the
  // recomputation runs synchronously but holds no per-operator lock during
  // BufferPoolSnapshot acquisition). Safe to call after invalidation
  // (no-op).
  void UpdateRemaining(ByteCount remainingBytes);

  // Marks this state as detached and freezes its decision to {0, true,
  // false}. Called by TemporaryMemoryManager during destruction so any
  // future Reservation/Decision calls are safe even though the manager is
  // gone. Never throws.
  void InvalidateForManagerDestruction() noexcept;

 private:
  friend class TemporaryMemoryManager;
  TemporaryMemoryState(
      TemporaryMemoryManager* manager,
      uint64_t id,
      TempMemoryRegisterOptions options);

  mutable std::mutex mutex_;
  TemporaryMemoryManager* manager_{nullptr};
  uint64_t id_{0};
  TempMemoryRegisterOptions options_;
  ReservationDecision decision_;
  bool invalidated_{false};
};

// Advisory-only temporary memory budget manager. It never reserves or
// allocates memory; it converts a BufferPoolSnapshot into conservative
// per-operator guidance using fair-share division
// (availableForOperators / max(active, 1)).
//
// Threading: all public methods are thread-safe. The manager keeps weak
// references to TemporaryMemoryState and locks them under its own mutex_;
// state mutexes are taken inside RecomputeBudgets while the manager mutex
// has been released, so the lock order is always
// state.mutex_ -> manager.mutex_ (never the reverse).
class TemporaryMemoryManager {
 public:
  // Creates an advisory budget manager backed by BufferPool snapshots.
  // 'pool' must outlive this manager; only Snapshot() is invoked on it.
  explicit TemporaryMemoryManager(BufferPool& pool);

  // Invalidates all live states before destruction so dangling
  // TemporaryMemoryState handles continue to work safely.
  ~TemporaryMemoryManager();

  // Registers one operator and returns a handle pre-populated with its
  // current decision. Triggers a RecomputeBudgets() so all active
  // operators see a fresh fair share. Throws BoltUserError if the manager
  // is shutting down.
  std::shared_ptr<TemporaryMemoryState> Register(
      TempMemoryRegisterOptions options);

  // Removes a state when its handle is destroyed. Called automatically by
  // TemporaryMemoryState's destructor. Never throws. Triggers a
  // RecomputeBudgets() so the remaining operators get a wider share.
  void Unregister(uint64_t id) noexcept;

  // Recomputes every live operator's decision from the latest BufferPool
  // snapshot. Idempotent and safe to call from any thread; call this after
  // a coordinator-level event that changes operator counts or the pool's
  // available headroom.
  void RecomputeBudgets();

  // Marks all states as detached and clears the registry. Called by the
  // destructor; tests may call it explicitly to simulate manager teardown.
  void InvalidateAllStatesForManagerDestruction() noexcept;

 private:
  friend class TemporaryMemoryState;
  // Pure helper: derives a decision from a frozen snapshot and the active
  // operator count without touching any member mutex, so callers can safely
  // invoke it while holding state_->mutex_. The fair share is
  //   floor(snapshot.availableForOperators / max(activeOperators, 1))
  // and the recommendation is clamped to options.estimatedRemainingBytes.
  static ReservationDecision ComputeDecision(
      const TempMemoryRegisterOptions& options,
      const BufferPoolSnapshot& snapshot,
      size_t activeOperators);

  BufferPool& pool_;
  mutable std::mutex mutex_;
  uint64_t nextStateId_{0};
  bool shuttingDown_{false};
  std::unordered_map<uint64_t, std::weak_ptr<TemporaryMemoryState>> states_;
};

} // namespace bytedance::bolt::memory::bm
