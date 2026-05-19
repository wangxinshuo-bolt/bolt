/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>

#include "bolt/common/memory/bm/ProcessSpillService.h"
#include "bolt/common/memory/bm/Types.h"

namespace bytedance::bolt::memory::bm::test {

// Returns a fresh per-test directory under the system temp dir. Old contents
// are wiped so successive runs do not accumulate stale spill files.
inline std::string testSpillDir(const std::string& name) {
  auto path = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path.string();
}

// Test fixture helper: lazily install the ProcessSpillService singleton with
// a single root directory the suite can extend per BufferManager. Subsequent
// callers reuse the same configuration so the tests stay isolated from each
// other but still respect the "ConfigureDefault may be called only once"
// rule (design doc §11). Returns the root directory the suite is rooted in.
inline std::string ensureTestSpillService() {
  static std::once_flag flag;
  static std::string root;
  std::call_once(flag, [&] {
    root = testSpillDir("bolt_bm_test_spill_root");
    DiskIoConfig ioConfig;
    ioConfig.backend = DiskIoBackend::kSync;
    ioConfig.initialQueueDepth = 4;
    ioConfig.minQueueDepth = 1;
    ioConfig.maxQueueDepth = 16;
    ProcessDiskIoService::ConfigureDefault(ioConfig);

    ProcessSpillServiceConfig cfg;
    SpillDirConfig dir;
    dir.path = root;
    cfg.dirs.push_back(std::move(dir));
    cfg.workerThreadCount = 0; // synchronous spill keeps unit tests deterministic
    cfg.cleanupOnDestroy = true;
    cfg.diskProbeDuration = std::chrono::milliseconds(0);
    ProcessSpillService::ConfigureDefault(std::move(cfg));
  });
  return root;
}

// Constructs a SpillClientConfig that enables spill participation for a
// BufferManager. The tenantId is used purely for metric labels.
inline SpillClientConfig makeSpillClientConfig(const std::string& tenantId) {
  ensureTestSpillService();
  SpillClientConfig cfg;
  cfg.enableSpill = true;
  cfg.tenantId = tenantId;
  return cfg;
}

} // namespace bytedance::bolt::memory::bm::test
