/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SmallSpillAllocator.h"

#include <algorithm>
#include <filesystem>
#include <utility>

#include <fmt/format.h>

namespace bytedance::bolt::memory::bm {
namespace {

SmallSpillConfig DefaultSmallSpillConfig(DiskKind disk) {
  SmallSpillConfig config;
  config.enabled = true;
  config.dedicatedFileThresholdBytes = 4 << 20;
  switch (disk) {
    case DiskKind::kNvme:
      config.slabFileBytes = 256 << 20;
      break;
    case DiskKind::kSsd:
      config.slabFileBytes = 128 << 20;
      break;
    case DiskKind::kHdd:
    case DiskKind::kNetworkFs:
    case DiskKind::kUnknown:
      config.slabFileBytes = 64 << 20;
      break;
  }
  config.sizeClasses = {
      4 << 10,
      8 << 10,
      16 << 10,
      32 << 10,
      64 << 10,
      128 << 10,
      256 << 10,
      512 << 10,
      1 << 20,
      2 << 20,
      4 << 20};
  return config;
}

} // namespace

SmallSpillConfig NormalizeSmallSpillConfig(
    SmallSpillConfig config,
    DiskKind disk) {
  auto defaults = DefaultSmallSpillConfig(disk);
  if (config.dedicatedFileThresholdBytes == 0) {
    config.dedicatedFileThresholdBytes = defaults.dedicatedFileThresholdBytes;
  }
  if (config.slabFileBytes == 0) {
    config.slabFileBytes = defaults.slabFileBytes;
  }
  if (config.sizeClasses.empty()) {
    config.sizeClasses = std::move(defaults.sizeClasses);
  }
  std::sort(config.sizeClasses.begin(), config.sizeClasses.end());
  config.sizeClasses.erase(
      std::unique(config.sizeClasses.begin(), config.sizeClasses.end()),
      config.sizeClasses.end());
  config.sizeClasses.erase(
      std::remove(config.sizeClasses.begin(), config.sizeClasses.end(), 0),
      config.sizeClasses.end());
  config.sizeClasses.erase(
      std::remove_if(
          config.sizeClasses.begin(),
          config.sizeClasses.end(),
          [&](ByteCount slotBytes) {
            return slotBytes > config.dedicatedFileThresholdBytes;
          }),
      config.sizeClasses.end());
  return config;
}

SmallSpillAllocator::SmallSpillAllocator(
    SmallSpillConfig config,
    DiskKind disk,
    std::string spillDir)
    : spillDir_(std::move(spillDir)),
      config_(NormalizeSmallSpillConfig(std::move(config), disk)) {
  if (!config_.enabled) {
    return;
  }
  for (const auto slotBytes : config_.sizeClasses) {
    if (slotBytes == 0 || slotBytes > config_.dedicatedFileThresholdBytes) {
      continue;
    }
    classes_.push_back(SmallSizeClass{
        slotBytes,
        std::max<ByteCount>(slotBytes, config_.slabFileBytes),
        {}});
  }
}

SpillLocation SmallSpillAllocator::Allocate(
    ByteCount logicalBytes,
    ByteCount storedBytes,
    SpillCompressionCodec codec,
    DiskKind disk) {
  const auto classIndex = ClassFor(storedBytes);
  if (!classIndex.has_value()) {
    return SpillLocation{};
  }

  std::lock_guard<std::mutex> l(mutex_);
  auto& cls = classes_[*classIndex];
  SmallSlabFile* selected{nullptr};
  uint64_t slotIndex = 0;
  for (auto& slab : cls.slabs) {
    if (slab.deleted) {
      continue;
    }
    if (!slab.freeSlots.empty()) {
      selected = &slab;
      slotIndex = slab.freeSlots.front();
      slab.freeSlots.pop_front();
      break;
    }
    if (slab.nextSlot < slab.totalSlots) {
      selected = &slab;
      slotIndex = slab.nextSlot++;
      break;
    }
  }
  if (selected == nullptr) {
    const auto slots =
        std::max<uint64_t>(1, cls.slabFileBytes / cls.slotBytes);
    cls.slabs.push_back(SmallSlabFile{
        MakeSlabPath(cls.slotBytes),
        cls.slotBytes,
        1,
        0,
        slots,
        false,
        {}});
    selected = &cls.slabs.back();
    slotIndex = 0;
  }
  ++selected->usedSlots;
  return SpillLocation{
      selected->path,
      slotIndex * selected->slotBytes,
      logicalBytes,
      storedBytes,
      selected->slotBytes,
      true,
      codec,
      disk};
}

bool SmallSpillAllocator::Release(const SpillLocation& location) {
  if (!location.smallSlot || location.slotBytes == 0) {
    return false;
  }

  std::lock_guard<std::mutex> l(mutex_);
  const auto classIndex = ClassFor(location.slotBytes);
  if (!classIndex.has_value()) {
    return false;
  }
  const auto slotIndex = location.offset / location.slotBytes;
  auto& cls = classes_[*classIndex];
  for (auto& slab : cls.slabs) {
    if (!slab.deleted && slab.path == location.path) {
      if (slab.usedSlots > 0) {
        --slab.usedSlots;
      }
      slab.freeSlots.push_back(slotIndex);
      if (slab.usedSlots == 0) {
        slab.deleted = true;
        return true;
      }
      return false;
    }
  }
  return false;
}

bool SmallSpillAllocator::Rollback(const SpillLocation& location) noexcept {
  if (!location.smallSlot || location.slotBytes == 0) {
    return false;
  }

  std::lock_guard<std::mutex> l(mutex_);
  const auto classIndex = ClassFor(location.slotBytes);
  if (!classIndex.has_value()) {
    return false;
  }
  const auto slotIndex = location.offset / location.slotBytes;
  auto& cls = classes_[*classIndex];
  for (auto& slab : cls.slabs) {
    if (!slab.deleted && slab.path == location.path) {
      if (slab.usedSlots > 0) {
        --slab.usedSlots;
      }
      slab.freeSlots.push_back(slotIndex);
      if (slab.usedSlots == 0) {
        slab.deleted = true;
        return true;
      }
      return false;
    }
  }
  return false;
}

std::optional<size_t> SmallSpillAllocator::ClassFor(ByteCount bytes) const {
  if (!config_.enabled || bytes == 0 ||
      bytes > config_.dedicatedFileThresholdBytes) {
    return std::nullopt;
  }
  for (size_t i = 0; i < classes_.size(); ++i) {
    if (bytes <= classes_[i].slotBytes) {
      return i;
    }
  }
  return std::nullopt;
}

std::string SmallSpillAllocator::MakeSlabPath(ByteCount slotBytes) {
  return (std::filesystem::path(spillDir_) /
          fmt::format("bm_small_{}_{}.spill", slotBytes, nextSlabId_++))
      .string();
}

} // namespace bytedance::bolt::memory::bm
