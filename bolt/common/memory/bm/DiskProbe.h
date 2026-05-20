/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "bolt/common/memory/bm/SpillTypes.h"

namespace bytedance::bolt::memory::bm {

struct DiskProbeConfig {
  std::string directory;
  std::chrono::milliseconds duration{std::chrono::seconds(1)};
  DiskKind forcedKind{DiskKind::kUnknown};
  DiskKind fallbackKind{DiskKind::kHdd};
};

struct DiskProbeResult {
  DiskKind kind{DiskKind::kUnknown};
  uint64_t writeIops{0};
  uint64_t readIops{0};
  uint64_t targetP95LatencyUs{50'000};
  bool activeProbeRan{false};
  bool directIoUsed{false};
};

DiskProbeResult ProbeDisk(const DiskProbeConfig& config);

uint64_t TargetP95LatencyForDisk(DiskKind kind);

} // namespace bytedance::bolt::memory::bm
