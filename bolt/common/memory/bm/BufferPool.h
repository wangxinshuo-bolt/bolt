/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <array>
#include <functional>
#include <memory>
#include <mutex>

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/Types.h"

namespace bytedance::bolt::memory::bm {

class QuotaSink;

// RAII handle for a quota charge granted by a QuotaSink (in production this
// is BufferPool). Destroying or Reset()'ing the handle returns the charged
// bytes to the sink. Default-constructed handles are "empty" (Size()==0,
// Tag()==kInternal) and Reset()/dtor have no effect on them.
class BufferPoolReservation {
 public:
  // Creates an empty reservation that owns no charge.
  BufferPoolReservation() = default;

  // Wraps an already-granted quota charge into an RAII reservation. Used
  // internally by BufferPool::Reserve; external callers should use Reserve()
  // to acquire reservations.
  BufferPoolReservation(
      QuotaSink* sink,
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind);

  // Releases the reservation back to the quota sink (no-op on empty handles).
  // Never throws; safe even when 'sink' has been destroyed if Reset() was
  // called first.
  ~BufferPoolReservation();

  // Transfers ownership from 'other'; 'other' becomes empty. The total bytes
  // charged in the sink is unchanged.
  BufferPoolReservation(BufferPoolReservation&& other) noexcept;

  // Releases this handle's existing charge, then takes ownership from
  // 'other'. 'other' becomes empty. Self-assignment is a no-op.
  BufferPoolReservation& operator=(BufferPoolReservation&& other) noexcept;

  BufferPoolReservation(const BufferPoolReservation&) = delete;
  BufferPoolReservation& operator=(const BufferPoolReservation&) = delete;

  // Changes the reservation size, charging or releasing only the delta.
  // Throws BoltUserError on an empty handle. When growing, propagates any
  // BoltMemAllocError from the underlying sink without modifying this
  // handle's state. newBytes == Size() is a no-op.
  void Resize(ByteCount newBytes);

  // Releases the full reservation and turns this handle into an empty one.
  // Idempotent and noexcept.
  void Reset() noexcept;

  // Returns the currently reserved bytes (0 for empty handles).
  ByteCount Size() const {
    return bytes_;
  }

  // Returns the accounting tag associated with this reservation. Defaults
  // to MemoryTag::kInternal on empty handles.
  MemoryTag Tag() const {
    return tag_;
  }

  // Returns the reservation class used for accounting limits. Defaults to
  // ReservationKind::kNormal on empty handles.
  ReservationKind Kind() const {
    return kind_;
  }

 private:
  QuotaSink* sink_{nullptr};
  MemoryTag tag_{MemoryTag::kInternal};
  ByteCount bytes_{0};
  ReservationKind kind_{ReservationKind::kNormal};
};

// Abstract logical-quota interface implemented by BufferPool. Tests can supply
// fakes; production code only sees QuotaSink references via
// BufferPoolReservation. All implementations are required to be thread-safe.
class QuotaSink {
 public:
  virtual ~QuotaSink() = default;

  // Attempts to charge 'bytes' against the sink under the supplied
  // (tag, kind) accounting buckets and returns an RAII reservation on
  // success. On failure the implementation MUST throw BoltMemAllocError;
  // it must not return an empty reservation. A bytes==0 request always
  // succeeds and returns a non-empty zero-byte reservation that still
  // routes Reset() back to this sink.
  virtual BufferPoolReservation Reserve(
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind) = 0;

  // Releases a previously granted charge. Required to be noexcept so
  // BufferPoolReservation's destructor and Reset() are safe in unwinding.
  // Implementations must clamp on underflow rather than throw, and must
  // accept bytes==0 as a no-op.
  virtual void Release(
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind) noexcept = 0;
};

// Logical quota pool used by BufferManager. BufferPool tracks how many bytes
// have been charged under each MemoryTag and ReservationKind, but never owns
// any physical memory itself. Physical allocation/free still goes through
// AccountedMemory / BufferAllocator. The pool is thread-safe; all public
// methods take an internal mutex and may be called from any thread.
//
// The pool enforces three independent limits:
//   * memoryLimitBytes -- total bytes across all kinds and tags
//   * pinnedLimitBytes -- additional cap on ReservationKind::kPinned
//   * emergencyScratchBytes -- a reserved tail of memoryLimitBytes that only
//     ReservationKind::kScratchEmergency may draw from. Non-emergency
//     reservations effectively see (memoryLimitBytes - emergencyScratchBytes).
//
// On the slow path Reserve() invokes the registered ReclaimFn (set by
// BufferManager) to try to free bytes via eviction/spill, then retries once.
// kScratchEmergency requests skip reclaim because they exist precisely for the
// reclaim path itself.
class BufferPool : public QuotaSink {
 public:
  // Reclaim callback installed by BufferManager. Called on slow-path Reserve
  // when usage would exceed a limit. Must attempt to free at least
  // 'targetBytes' synchronously and return the actual bytes freed (may be 0).
  // Implementations are forbidden from calling back into BufferPool::Reserve.
  using ReclaimFn = std::function<ByteCount(ByteCount targetBytes)>;

  // Creates a logical quota pool from BufferManager configuration limits.
  // memoryLimitBytes==0 in the config is interpreted as "unlimited"
  // (int64 max). pinnedLimitBytes==0 falls back to memoryLimitBytes.
  // No reclaimer is installed; call SetReclaimer() afterwards.
  explicit BufferPool(BufferManagerConfig config);

  // Reserves logical quota for 'bytes' under (tag, kind). On success returns
  // a non-empty BufferPoolReservation that releases the charge on destruction
  // or Reset(). bytes==0 succeeds without charging anything but still returns
  // a handle bound to this pool. tag must be < MemoryTag::kNumTags.
  //
  // Fast path: try to reserve under the lock, succeed and return.
  // Slow path: drop the lock, invoke the reclaimer (if any and kind !=
  //   kScratchEmergency), reacquire the lock, retry exactly once. If the
  //   retry still cannot satisfy the request, throws BoltMemAllocError.
  // Throws BoltUserError if tag is out of range.
  BufferPoolReservation Reserve(
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind) override;

  // Releases a charge previously granted by Reserve(). Called automatically
  // by BufferPoolReservation. Idempotent on bytes==0. Underflow is clamped:
  // if a buggy caller releases more than was charged, counters saturate at 0
  // and the call still succeeds (noexcept).
  void Release(
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind) noexcept override;

  // Returns a consistent point-in-time snapshot of all counters. Intended for
  // diagnostics, metrics, and the TemporaryMemoryManager budgeting math. The
  // snapshot satisfies the invariants documented on BufferPoolSnapshot.
  BufferPoolSnapshot Snapshot() const;

  // Returns total logical bytes currently charged across all tags/kinds.
  // Equivalent to Snapshot().usedTotalBytes.
  ByteCount GetMemoryUsage() const;

  // Returns logical bytes currently charged under one MemoryTag (sum across
  // ReservationKinds). Throws BoltUserError if tag is out of range.
  ByteCount GetMemoryUsage(MemoryTag tag) const;

  // Returns the configured logical memory limit. If the configuration set
  // memoryLimitBytes==0 (unlimited), this returns int64 max.
  ByteCount MemoryLimit() const;

  // Installs the slow-path reclaim callback. May be called multiple times;
  // the latest callback wins. Pass an empty std::function to disable
  // reclaim (the slow path then becomes "retry once and fail").
  void SetReclaimer(ReclaimFn reclaim);

 private:
  bool TryReserveLocked(MemoryTag tag, ByteCount bytes, ReservationKind kind);
  void ReleaseLocked(MemoryTag tag, ByteCount bytes, ReservationKind kind);

  const ByteCount memoryLimitBytes_;
  const ByteCount pinnedLimitBytes_;
  const ByteCount emergencyScratchBytes_;
  mutable std::mutex mutex_;
  ByteCount usedTotalBytes_{0};
  ByteCount usedPinnedBytes_{0};
  ByteCount usedScratchBytes_{0};
  ByteCount usedEmergencyScratchBytes_{0};
  std::array<ByteCount, static_cast<size_t>(MemoryTag::kNumTags)> usedByTag_{};
  ReclaimFn reclaim_;
};

// RAII wrapper that pairs a BufferPoolReservation (logical quota) with a
// physical allocation from a Bolt MemoryPool. AccountedMemory guarantees that
// quota and physical bytes are released together, even when the constructor
// throws partway through.
//
// Lifetime:
//   * Make() reserves quota first; if the physical allocation throws the
//     reservation is rolled back before the exception escapes.
//   * Destruction frees the physical bytes back to the MemoryPool and lets
//     the embedded reservation release the logical charge.
//
// Non-copyable and non-movable: callers store these inside std::unique_ptr.
class AccountedMemory {
 public:
  // Reserves BufferPool quota for 'bytes' under (tag, kind), then allocates
  // 'bytes' of physical memory from 'memoryPool'. bytes==0 returns a wrapper
  // with a null Data() pointer and Size()==0 (still owns a zero-byte
  // reservation, which is harmless).
  // Throws:
  //   BoltMemAllocError if BufferPool quota cannot be granted.
  //   Whatever 'memoryPool.allocate' throws if physical allocation fails;
  //   the reservation is released before the exception propagates.
  static std::unique_ptr<AccountedMemory> Make(
      BufferPool& pool,
      MemoryPool& memoryPool,
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind);

  // Frees physical bytes through the originating MemoryPool and releases
  // the embedded reservation. Never throws.
  ~AccountedMemory();

  AccountedMemory(AccountedMemory&&) = delete;
  AccountedMemory& operator=(AccountedMemory&&) = delete;
  AccountedMemory(const AccountedMemory&) = delete;
  AccountedMemory& operator=(const AccountedMemory&) = delete;

  // Returns a mutable pointer to the owned physical buffer of length Size().
  // Returns nullptr iff Size()==0. The pointer is valid until this object is
  // destroyed.
  DataPtr Data() {
    return data_;
  }

  // Const overload of Data().
  ConstDataPtr Data() const {
    return data_;
  }

  // Returns the allocation size in bytes (0 for the zero-byte case).
  ByteCount Size() const {
    return bytes_;
  }

 private:
  AccountedMemory(
      BufferPoolReservation reservation,
      MemoryPool& memoryPool,
      DataPtr data,
      ByteCount bytes);

  BufferPoolReservation reservation_;
  MemoryPool* memoryPool_{nullptr};
  DataPtr data_{nullptr};
  ByteCount bytes_{0};
};

// Convenience factory that hides the BufferPool/MemoryPool pair behind a
// single object. BufferAllocator does not own either pool; both must outlive
// the allocator and the AccountedMemory instances it produces.
class BufferAllocator {
 public:
  // Binds a logical BufferPool with a physical Bolt MemoryPool. References
  // are stored; both pools must outlive this allocator.
  BufferAllocator(BufferPool& pool, MemoryPool& memoryPool);

  // Allocates accounted memory under the requested ReservationKind. Equivalent
  // to AccountedMemory::Make(pool_, memoryPool_, tag, bytes, kind). See that
  // method for the throwing/rollback contract.
  std::unique_ptr<AccountedMemory> Allocate(
      MemoryTag tag,
      ByteCount bytes,
      ReservationKind kind = ReservationKind::kNormal);

  // Shortcut for Allocate(tag, bytes, ReservationKind::kScratch). Counts
  // toward usedScratchBytes in BufferPoolSnapshot.
  std::unique_ptr<AccountedMemory> AllocateScratch(
      MemoryTag tag,
      ByteCount bytes);

  // Shortcut for Allocate(tag, bytes, ReservationKind::kScratchEmergency).
  // Draws from the configured emergency-scratch headroom and bypasses the
  // reclaimer. Use only from spill/eviction paths to avoid recursive reclaim.
  // Throws BoltMemAllocError if emergencyScratchBytes is exhausted (or zero).
  std::unique_ptr<AccountedMemory> AllocateEmergencyScratch(
      MemoryTag tag,
      ByteCount bytes);

 private:
  BufferPool& pool_;
  MemoryPool& memoryPool_;
};

} // namespace bytedance::bolt::memory::bm
