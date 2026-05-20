/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/Types.h"

namespace bytedance::bolt::memory::bm {

// Process-wide spill service configuration. Pass via ConfigureDefault()
// before the first Instance() call. ConfigureDefault() must be called
// exactly once: any subsequent call (including a re-call after
// ResetForTesting) throws BoltUserError. Instance() also throws if no
// ConfigureDefault has been issued, mirroring the design doc rule that
// the service has no implicit defaults.
struct ProcessSpillServiceConfig {
  // Single spill directory. Must be non-empty.
  std::string spillDir;
  DiskKind forcedKind{DiskKind::kUnknown};
  // Worker thread pool size. 0 means no async workers; SubmitSpill returns
  // kBackpressured so the caller can fall back to synchronous SpillToDisk.
  uint32_t workerThreadCount{0};
  // Optional metrics sink shared by every component the service owns.
  MetricsRegistry* metrics{nullptr};
  // Default classification when probing returns kUnknown.
  DiskKind unknownFallbackKind{DiskKind::kHdd};
  // Whether to remove spill files when ProcessSpillService is destroyed.
  bool cleanupOnDestroy{true};
  // Active disk probe duration for each spill directory. Zero disables active
  // probing and uses the configured forced/fallback kind.
  std::chrono::milliseconds diskProbeDuration{std::chrono::seconds(1)};
  // Small spill block packing policy. When sizeClasses is empty, the store
  // derives defaults from the effective disk kind.
  SmallSpillConfig smallSpill;
  // Spill payload compression policy. Enabled by default.
  SpillCompressionConfig compression;
};

// Single process spill service. Owns the worker pool, file system view, and
// the progress epoch used by reclaim waiters.
//
// Lifecycle:
//   1) ConfigureDefault(cfg)   -> mandatory; throws on second call
//   2) Instance()              -> lazy init; throws if not configured
//   3) ResetForTesting()       -> tests only
//
// Shutdown is explicit: the destructor stops workers, syncs files, and
// best-effort cleans owned spill dirs. atexit is intentionally NOT used
// to avoid static destruction-order surprises.
//
// Thread-safety: all public methods are safe to call concurrently.
class ProcessSpillService : public SpillRequester {
 public:
  // Returns the (lazily initialized) singleton. The first call after
  // ConfigureDefault constructs the service; subsequent calls return the
  // same reference. Throws BoltUserError if ConfigureDefault has not been
  // called.
  static ProcessSpillService& Instance();

  // Installs the configuration used by lazy initialization. Must be called
  // exactly once per process (including across ResetForTesting cycles in
  // tests). Validates: 'config.spillDir' must be non-empty.
  // Throws BoltUserError on:
  //   * a second invocation while an Instance is alive
  //   * an empty spillDir.
  static void ConfigureDefault(ProcessSpillServiceConfig config);

  // Tests only: tears down the singleton so the next ConfigureDefault can
  // succeed. Safe to call when no Instance has been created.
  static void ResetForTesting();

  // Tests only: creates an isolated, non-singleton service. This is useful
  // for cases that need a different process-level configuration than the
  // singleton used by other tests.
  static std::unique_ptr<ProcessSpillService> CreateForTesting(
      ProcessSpillServiceConfig config);

  EvictResult SubmitSpill(EvictionNode node) override;

  bool WaitForProgress(
      ByteCount bytesNeeded,
      std::chrono::milliseconds timeout) override;

  // Snapshot of total disk bytes currently held by the process spill store.
  // Updated atomically by Write/Release; safe to read from any thread.
  ByteCount UsedDiskBytes() const {
    return usedDiskBytes_.load(std::memory_order_relaxed);
  }

  SpillLocation Write(MemoryTag tag, ConstDataPtr src, ByteCount bytes);

  void Read(const SpillLocation& location, DataPtr dst, ByteCount dstCapacity);

  void Release(const SpillLocation& location) noexcept;

  ProcessSpillService(const ProcessSpillService&) = delete;
  ProcessSpillService& operator=(const ProcessSpillService&) = delete;

 private:
  // unique_ptr<ProcessSpillService> in the singleton holder needs to invoke
  // our destructor; expose it via std::default_delete without leaking the
  // dtor into the public API.
  friend struct std::default_delete<ProcessSpillService>;

  // Use Instance() / ResetForTesting() / ConfigureDefault().
  explicit ProcessSpillService(ProcessSpillServiceConfig config);
  ~ProcessSpillService();

  void StartWorkers();
  void StopWorkers();
  void WorkerLoop();
  void NotifyProgress();
  void ExecuteSpill(EvictionNode node);

  // Best-effort cleanup of <root>/bolt_spill_<pid>_* directories whose pid
  // is no longer alive. Called once per process at construction time. Any
  // I/O failure is logged but not propagated.
  void CleanupStaleDirsAtStartup();

  ProcessSpillServiceConfig config_;
  MetricsRegistry& metrics_;
  std::unique_ptr<SpillStore> store_;
  std::atomic<ByteCount> usedDiskBytes_{0};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::condition_variable progressCv_;
  std::deque<EvictionNode> ready_;
  bool stopping_{false};
  bool started_{false};
  uint64_t progressEpoch_{0};
  std::vector<std::thread> workers_;
};

} // namespace bytedance::bolt::memory::bm
