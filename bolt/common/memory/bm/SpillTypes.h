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

// Configuration of one spill directory entry. The user may force a disk
// (overriding probing) and supply per-directory tuning. 'path' must be a
// non-empty filesystem path; SpillStore creates the directory at startup
// if it does not exist (parents must already exist).
struct SpillDirConfig {
  // Non-empty path. Must point at a directory the process may create
  // sub-paths under. Empty path is rejected by ProcessSpillService.
  std::string path;
  // When set to anything other than kUnknown the value overrides probing
  // and is reported by SpillStore::Disk() / SpillLocation::disk.
  DiskKind forcedKind{DiskKind::kUnknown};
};

// Per-tenant configuration of one SpillClient bound to ProcessSpillService.
// Each BufferManager owns exactly one SpillClient and must populate this in
// BufferManagerConfig::spillClient (no defaults: spilling is opt-in).
struct SpillClientConfig {
  // When false, BufferManager runs in spill-disabled mode:
  //   - kSpillToDisk and kCompressThenSpill allocations throw at Allocate()
  //     time ("Spill policies require SpillClientConfig::enableSpill = true")
  //   - no SpillClient is registered with ProcessSpillService
  //   - BufferManager::Spill() returns nullptr
  //   - Reclaim only visits cheap policies (kDiscard / kRecompute).
  bool enableSpill{false};
  // Stable tenant identifier for metric labels and diagnostics. The service
  // does NOT enforce uniqueness; multiple clients may share the same id.
  std::string tenantId;
  // 0 means unlimited; Write() that would push the client's UsedDiskBytes()
  // above this limit fails fast with BoltUserError ("client disk quota
  // exceeded"). The process-level quota is checked first and is rolled back
  // on client-quota overflow.
  ByteCount diskQuotaBytes{0};
  // DRF-lite weight passed to GlobalSpillScheduler. Higher weight => smaller
  // virtualTime increment per scheduled byte => proportionally more
  // bandwidth. 0 is normalized to 1.
  uint64_t fairnessWeight{1};
  // Default priority assigned to nodes that do not specify their own.
  // Currently informational; reserved for future scheduling tweaks.
  Priority defaultPriority{Priority::kNormal};
};

} // namespace bytedance::bolt::memory::bm
