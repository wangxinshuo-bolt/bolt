/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

#include "bolt/common/memory/bm/MemoryTypes.h"
#include "bolt/common/memory/bm/Metrics.h"
#include "bolt/common/memory/bm/SpillTypes.h"

namespace bytedance::bolt::memory::bm {

// Point-in-time view of one BufferPool returned by Snapshot(). All fields
// are byte counts; ratios are not pre-computed. Invariants:
//   usedTotalBytes        == sum over kinds (normal+pinned+scratch+emergency)
//   usedPinnedBytes       <= pinnedLimitBytes
//   usedEmergencyScratch  <= emergencyScratchBytes
//   usedScratchBytes      includes BOTH scratch and scratch_emergency
//   operatorMemoryLimit   == max(0, memoryLimit - emergencyScratch)
//   availableForOperators == max(0, operatorMemoryLimit - usedTotalBytes)
struct BufferPoolSnapshot {
  ByteCount memoryLimitBytes{0};
  ByteCount operatorMemoryLimitBytes{0};
  ByteCount pinnedLimitBytes{0};
  ByteCount usedTotalBytes{0};
  ByteCount usedPinnedBytes{0};
  ByteCount usedScratchBytes{0};
  ByteCount usedEmergencyScratchBytes{0};
  ByteCount emergencyScratchBytes{0};
  ByteCount availableForOperators{0};
};

// Configuration knob bundle passed to BufferManager's constructor. Defaults
// produce a "no-quota, no-spill" manager useful only for trivial tests; real
// callers should set memoryLimitBytes and (optionally) spillClient.
struct BufferManagerConfig {
  // Total logical byte budget enforced by BufferPool. 0 disables the limit
  // (treated as effectively unlimited). When non-zero, allocations whose
  // sum would exceed (memoryLimitBytes - emergencyScratchBytes) trigger
  // the slow-path reclaimer, then throw BoltMemAllocError on failure.
  ByteCount memoryLimitBytes{0};
  // Sub-budget on kPinned reservations. 0 means "share the full memoryLimit".
  // Allocate / Reserve with kPinned throw BoltMemAllocError when exceeded.
  ByteCount pinnedLimitBytes{0};
  // When zero, BufferManager auto-derives emergency scratch from the
  // configured limit using emergencyScratchFraction / emergencyScratchFloor
  // and the 256 MiB gate (§14.1): below 256 MiB total, emergency scratch
  // is forced to 0 and spill policies are rejected at Allocate() time.
  ByteCount emergencyScratchBytes{0};
  // Name registered with the underlying Bolt MemoryPool. An empty string
  // is auto-suffixed with a unique id; non-empty strings are used as-is.
  std::string poolName{"__buffer_manager__"};

  // Per design doc §13. Maximum time Reclaim() will block waiting for an
  // async spill to make progress before returning the bytes-freed value
  // it has so far.
  std::chrono::milliseconds reserveWaitTimeout{std::chrono::milliseconds(1000)};
  // Auto-derive ratio for emergency scratch. Clamped to [0,1] internally.
  // Used only when emergencyScratchBytes == 0 AND memoryLimitBytes >= 256MiB.
  double emergencyScratchFraction{0.005};
  // Lower bound applied after emergencyScratchFraction multiplication.
  ByteCount emergencyScratchFloor{64ULL << 20};

  // Per-tenant spill participation. Disk I/O lives in the process-wide
  // ProcessSpillService; this configures only the per-BufferManager view.
  // Leave default-constructed (enableSpill = false) for memory-only mode.
  SpillClientConfig spillClient;
  // Optional metrics sink used for BufferPool / BlockHandle counters. The
  // ProcessSpillService is configured separately via ConfigureDefault().
  // nullptr installs NoOpMetricsRegistry().
  MetricsRegistry* metrics{nullptr};
};

// Parameters supplied at block creation time. After Allocate() returns, the
// values are frozen on the BlockHandle.
struct AllocateOptions {
  // Accounting tag. Must NOT be MemoryTag::kNumTags.
  MemoryTag tag{MemoryTag::kHashTable};
  // Block payload size in bytes. Must be > 0; Allocate throws otherwise.
  ByteCount size{0};
  // Externalization strategy. See EvictPolicy for the per-policy contract.
  // kSpillToDisk / kCompressThenSpill require:
  //   - BufferManagerConfig.spillClient.enableSpill == true
  //   - non-zero emergencyScratchBytes (auto or explicit)
  // kRecompute additionally requires recoveryFn != nullptr.
  EvictPolicy policy{EvictPolicy::kDiscard};
  // Importance hint used by the eviction queue. Higher priority blocks are
  // popped LATER within the same cost class.
  Priority priority{Priority::kNormal};
  // Required iff policy == kRecompute. Invoked by Pin() when reloading a
  // block that was previously evicted. The buffer passed in is the freshly
  // allocated body of exactly 'size' bytes; the function must fully populate
  // it and may throw to signal recompute failure.
  std::function<void(DataPtr, ByteCount)> recoveryFn;
};

// Reserve-modifier (currently informational, reserved for future API).
//   kStrict     – fail fast when the request cannot be honored exactly.
//   kBestEffort – grant whatever the pool can satisfy and return that.
enum class SetLimitMode : uint8_t {
  kStrict,
  kBestEffort,
};

} // namespace bytedance::bolt::memory::bm
