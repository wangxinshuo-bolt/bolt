/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include <string>

#include "bolt/common/memory/bm/MemoryTypes.h"

namespace bytedance::bolt::memory::bm {

// Coarse storage disk classification used to select per-directory spill
// profiles (concentration of disk-aware logic is contained in SpillStore,
// per design doc §10.2). kUnknown only appears as an intermediate value:
// after SpillStore construction, Disk() always returns one of the
// concrete kinds (forced > probed > config.unknownFallbackKind).
enum class DiskKind : uint8_t {
  kUnknown,
  kHdd,
  kSsd,
  kNvme,
  kNetworkFs,
};

// Returns a stable lower-case debug string for 'kind' (e.g. "unknown",
// "hdd", "ssd", "nvme", "network_fs"). Never throws.
const char* ToString(DiskKind kind);

} // namespace bytedance::bolt::memory::bm
