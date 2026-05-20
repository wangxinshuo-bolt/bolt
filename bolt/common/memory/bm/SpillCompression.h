/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstddef>
#include <vector>

#include "bolt/common/memory/bm/SpillLocation.h"

namespace bytedance::bolt::memory::bm {

struct SpillCompressionConfig {
  bool enabled{true};
  SpillCompressionCodec codec{SpillCompressionCodec::kZstd};
  int level{1};
  ByteCount minBytes{0};
  double minSavingsRatio{0.05};
};

struct PreparedSpillPayload {
  ConstDataPtr data{nullptr};
  ByteCount storedBytes{0};
  SpillCompressionCodec codec{SpillCompressionCodec::kNone};
  std::vector<uint8_t> compressed;
};

PreparedSpillPayload PrepareSpillPayload(
    const SpillCompressionConfig& config,
    ConstDataPtr src,
    ByteCount bytes);

void DecompressSpillPayload(
    const SpillLocation& location,
    ConstDataPtr compressed,
    DataPtr dst);

} // namespace bytedance::bolt::memory::bm
