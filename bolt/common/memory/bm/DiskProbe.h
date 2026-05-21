/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

#include "bolt/common/memory/bm/SpillTypes.h"

namespace bytedance::bolt::memory::bm {

struct DiskProbeConfig {
  std::string directory;
  std::chrono::milliseconds duration{std::chrono::seconds(1)};
  DiskKind forcedKind{DiskKind::kUnknown};
  DiskKind fallbackKind{DiskKind::kHdd};
  size_t blockBytes{4096};
  uint64_t probeFileBytes{64ULL * 1024ULL * 1024ULL};
  uint64_t nvmeMinIops{40'000};
  uint64_t ssdMinIops{2'000};
  uint64_t writeFsyncEveryOps{64};
  uint64_t offsetStride{104'729};
};

struct DiskProbeResult {
  DiskKind kind{DiskKind::kUnknown};
  uint64_t writeIops{0};
  uint64_t readIops{0};
  bool activeProbeRan{false};
  bool directIoUsed{false};
};

DiskProbeResult ProbeDisk(const DiskProbeConfig& config);

} // namespace bytedance::bolt::memory::bm
