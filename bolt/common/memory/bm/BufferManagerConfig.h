/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

#include "bolt/common/memory/bm/DiskIo.h"
#include "bolt/common/memory/bm/DiskProbe.h"
#include "bolt/common/memory/bm/MemoryTypes.h"
#include "bolt/common/memory/bm/Metrics.h"
#include "bolt/common/memory/bm/SmallSpillAllocator.h"
#include "bolt/common/memory/bm/SpillCompression.h"
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

// Best-effort prefetch controls. Prefetch reloads spilled blocks into
// resident memory without pinning them.
struct PrefetchOptions {
  Priority priority{Priority::kNormal};
  bool bestEffort{true};
};

// Submission summary returned by BufferManager::Prefetch().
struct PrefetchResult {
  uint64_t submittedCount{0};
  uint64_t skippedCount{0};
  uint64_t alreadyLoadedCount{0};
  uint64_t backpressuredCount{0};
};

// Process-level spill configuration supplied through
// BufferManager::InitializeProcessServices(). This owns executor/process-wide
// resources: spill directory, disk probing, disk I/O scheduling, small-block
// layout, compression, and spill task workers. BufferManager owner threads prepare
// and commit state; process spill task workers execute the disk I/O.
struct BufferManagerProcessSpillConfig {
  bool enabled{true};
  std::string spillDir;
  DiskKind forcedKind{DiskKind::kUnknown};
  uint32_t workerThreadCount{1};
  DiskKind unknownFallbackKind{DiskKind::kHdd};
  bool cleanupOnDestroy{true};
  std::chrono::milliseconds diskProbeDuration{std::chrono::seconds(1)};
  DiskProbeConfig diskProbe;
  DiskIoConfig diskIo;
  SmallSpillConfig smallSpill;
  SpillCompressionConfig compression;
};

// Process-level service configuration. Call
// BufferManager::InitializeProcessServices() once before any BufferManager uses
// EvictPolicy::kSpillToDisk.
struct BufferManagerProcessServicesConfig {
  BufferManagerProcessSpillConfig spill;
  MetricsRegistry* metrics{nullptr};
};

// Configuration knob bundle passed to BufferManager's constructor.
struct BufferManagerConfig {
  // Name registered with the underlying Bolt MemoryPool. An empty string
  // is auto-suffixed with a unique id; non-empty strings are used as-is.
  std::string poolName{"__buffer_manager__"};

  // Maximum time Reclaim() will block waiting for an async spill to make
  // progress before returning the bytes-freed value it has so far.
  std::chrono::milliseconds reserveWaitTimeout{std::chrono::milliseconds(1000)};

  // Optional metrics sink used for this BufferManager's BufferPool /
  // BlockHandle counters. Process-level services receive their metrics sink
  // through BufferManagerProcessServicesConfig.
  MetricsRegistry* metrics{nullptr};

  // Whether this BufferManager may allocate blocks with
  // EvictPolicy::kSpillToDisk. The process spill coordinator must still be
  // initialized explicitly before such allocations.
  bool spillEnabled{true};
};

// Parameters supplied at block creation time. After Allocate() returns, the
// values are frozen on the BlockHandle.
struct AllocateOptions {
  // Accounting tag. Must NOT be MemoryTag::kNumTags.
  MemoryTag tag{MemoryTag::kHashTable};
  // Block payload size in bytes. Must be > 0; Allocate throws otherwise.
  ByteCount size{0};
  // Externalization strategy. kSpillToDisk requires
  // BufferManager::InitializeProcessServices() to be called before Allocate().
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
