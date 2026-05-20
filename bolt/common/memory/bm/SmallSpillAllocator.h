/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "bolt/common/memory/bm/DiskProbe.h"
#include "bolt/common/memory/bm/SpillLocation.h"

namespace bytedance::bolt::memory::bm {

struct SmallSpillConfig {
  bool enabled{true};
  ByteCount dedicatedFileThresholdBytes{0};
  ByteCount slabFileBytes{0};
  std::vector<ByteCount> sizeClasses;
};

class SmallSpillAllocator {
 public:
  SmallSpillAllocator(
      SmallSpillConfig config,
      DiskKind disk,
      std::string spillDir);

  const SmallSpillConfig& Config() const {
    return config_;
  }

  SpillLocation Allocate(
      ByteCount logicalBytes,
      ByteCount storedBytes,
      SpillCompressionCodec codec,
      DiskKind disk);

  bool Release(const SpillLocation& location);
  bool Rollback(const SpillLocation& location) noexcept;

 private:
  struct SmallSlabFile {
    std::string path;
    ByteCount slotBytes{0};
    uint64_t nextSlot{0};
    uint64_t usedSlots{0};
    uint64_t totalSlots{0};
    bool deleted{false};
    std::deque<uint64_t> freeSlots;
  };

  struct SmallSizeClass {
    ByteCount slotBytes{0};
    ByteCount slabFileBytes{0};
    std::vector<SmallSlabFile> slabs;
  };

  std::optional<size_t> ClassFor(ByteCount bytes) const;
  std::string MakeSlabPath(ByteCount slotBytes);

  const std::string spillDir_;
  const SmallSpillConfig config_;
  mutable std::mutex mutex_;
  std::atomic<uint64_t> nextSlabId_{0};
  std::vector<SmallSizeClass> classes_;
};

SmallSpillConfig NormalizeSmallSpillConfig(
    SmallSpillConfig config,
    DiskKind disk);

} // namespace bytedance::bolt::memory::bm
