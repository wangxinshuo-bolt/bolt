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

// Point-in-time view of one BufferPool returned by Snapshot(). BufferPool is
// observational only: it records usage by kind/tag but does not enforce quota.
struct BufferPoolSnapshot {
  ByteCount usedTotalBytes{0};
  ByteCount usedPinnedBytes{0};
  ByteCount usedLoadedBytes{0};
  ByteCount usedSpilledBytes{0};
};

// Configuration knob bundle passed to BufferManager's constructor.
struct BufferManagerConfig {
  // Name registered with the underlying Bolt MemoryPool. An empty string
  // is auto-suffixed with a unique id; non-empty strings are used as-is.
  std::string poolName{"__buffer_manager__"};

  // Maximum time Reclaim() will block waiting for an async spill to make
  // progress before returning the bytes-freed value it has so far.
  std::chrono::milliseconds reserveWaitTimeout{std::chrono::milliseconds(1000)};

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
  // Externalization strategy. kSpillToDisk requires ProcessSpillService to
  // be configured before Allocate().
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
