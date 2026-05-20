/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include <string>

#include "bolt/common/memory/bm/MemoryTypes.h"
#include "bolt/common/memory/bm/SpillTypes.h"

namespace bytedance::bolt::memory::bm {

enum class SpillCompressionCodec : uint8_t {
  kNone = 0,
  kZstd = 1,
};

struct SpillLocation {
  std::string path;
  uint64_t offset{0};
  ByteCount logicalBytes{0};
  ByteCount storedBytes{0};
  ByteCount slotBytes{0};
  bool smallSlot{false};
  SpillCompressionCodec compressionCodec{SpillCompressionCodec::kNone};
  DiskKind disk{DiskKind::kUnknown};

  bool Valid() const {
    return !path.empty();
  }
};

} // namespace bytedance::bolt::memory::bm
