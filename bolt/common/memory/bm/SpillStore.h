/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "bolt/common/memory/bm/DiskIo.h"
#include "bolt/common/memory/bm/DiskProbe.h"
#include "bolt/common/memory/bm/MemoryTypes.h"
#include "bolt/common/memory/bm/Metrics.h"
#include "bolt/common/memory/bm/SmallSpillAllocator.h"
#include "bolt/common/memory/bm/SpillCompression.h"
#include "bolt/common/memory/bm/SpillLocation.h"

namespace bytedance::bolt::memory::bm {

class SpillStore;

// Configuration for the process-wide BufferManager spill directory.
struct SpillStoreConfig {
  std::string spillDir;
  bool cleanupOnDestroy{true};
  DiskKind forcedKind{DiskKind::kUnknown};
  DiskKind unknownFallbackKind{DiskKind::kHdd};
  DiskProbeResult diskProbe;
  SmallSpillConfig smallSpill;
  SpillCompressionConfig compression;
};

// RAII handle for a single spill write attempt. At most one Write() may
// succeed; failed writes clean up their partial bookkeeping before throwing.
class SpillWriteSession {
 public:
  SpillWriteSession(SpillWriteSession&& other) noexcept;
  SpillWriteSession& operator=(SpillWriteSession&& other) noexcept;
  SpillWriteSession(const SpillWriteSession&) = delete;
  SpillWriteSession& operator=(const SpillWriteSession&) = delete;

  ~SpillWriteSession() noexcept;

  SpillLocation Write(MemoryTag tag, ConstDataPtr src, ByteCount bytes);

 private:
  friend class SpillStore;
  SpillWriteSession(SpillStore* store, DiskKind disk);

  SpillStore* store_{nullptr};
  DiskKind disk_{DiskKind::kUnknown};
  bool consumed_{false};
};

// File-backed spill store for immutable BufferManager blocks.
class SpillStore {
 public:
  SpillStore(
      SpillStoreConfig config,
      MetricsRegistry* metrics = nullptr,
      DiskIoScheduler* ioScheduler = nullptr);

  ~SpillStore();

  SpillWriteSession BeginWriteAttempt(MemoryTag tag, bool allowCompression);

  SpillLocation Write(MemoryTag tag, ConstDataPtr data, ByteCount bytes);

  void Read(const SpillLocation& location, DataPtr dst, ByteCount dstCapacity);

  void Release(const SpillLocation& location);

  // Returns the disk effective for this store after construction-time
  // probing and config overrides. Stable for the store's lifetime.
  DiskKind Disk() const {
    return disk_;
  }

  static void CleanupAtStartup(const SpillStoreConfig& cfg);

 private:
  friend class SpillWriteSession;

  std::string MakeDedicatedPath(MemoryTag tag);

  std::string LocationKey(const SpillLocation& location) const;
  std::string LocationKey(const std::string& path, uint64_t offset) const;
  SpillLocation AllocateDedicated(
      MemoryTag tag,
      ByteCount logicalBytes,
      ByteCount storedBytes,
      SpillCompressionCodec codec);
  void WriteToLocation(const SpillLocation& location, ConstDataPtr src);

  void RegisterLiveFile(const std::string& path);

  bool ForgetLiveFile(const std::string& path) noexcept;

  const SpillStoreConfig config_;
  MetricsRegistry& metrics_;
  DiskIoScheduler* ioScheduler_;
  const DiskProbeResult diskProbe_;
  const SpillCompressionConfig compressionConfig_;
  SmallSpillAllocator smallAllocator_;
  Counter& bytesWrittenCounter_;
  Counter& bytesReadCounter_;
  Counter& doubleReleaseCounter_;
  Counter& invalidReleaseCounter_;
  const DiskKind disk_;
  mutable std::mutex mutex_;
  std::atomic<uint64_t> nextFileId_{0};
  std::unordered_set<std::string> liveFiles_;
  std::unordered_set<std::string> liveLocations_;
  std::unordered_set<std::string> releasedFiles_;
};

} // namespace bytedance::bolt::memory::bm
