/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include <string>

#include "bolt/common/memory/bm/MemoryTypes.h"

namespace bytedance::bolt::memory::bm {

// Coarse storage disk classification used to select spill defaults.
// kUnknown only appears as an intermediate value; after SpillFileStore
// construction, Disk() returns a concrete kind selected from forced, probed,
// or fallback configuration.
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
