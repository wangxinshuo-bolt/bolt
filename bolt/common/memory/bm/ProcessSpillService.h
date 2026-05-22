/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "bolt/common/memory/bm/BufferManagerConfig.h"
#include "bolt/common/memory/bm/DiskIo.h"
#include "bolt/common/memory/bm/EvictionTypes.h"
#include "bolt/common/memory/bm/Metrics.h"
#include "bolt/common/memory/bm/BufferPool.h"
#include "bolt/common/memory/bm/SpillStore.h"

namespace bytedance::bolt::memory::bm {

class BufferManager;
struct SpillOwnerToken {};

// Process-wide spill service configuration. Production callers install this
// through BufferManager::InitializeProcessServices(); ConfigureDefault() is
// kept as the internal singleton hook. Instance() throws if no configuration
// has been installed, because the service has no implicit process defaults.
struct ProcessSpillServiceConfig {
  // Single spill directory. Must be non-empty.
  std::string spillDir;
  DiskKind forcedKind{DiskKind::kUnknown};
  // Which thread executes spill work. kWorkerThread requires
  // workerThreadCount > 0 and never falls back to owner-thread writes.
  SpillExecutionMode executionMode{SpillExecutionMode::kWorkerThread};
  uint32_t workerThreadCount{1};
  // Optional metrics sink shared by every component the service owns.
  MetricsRegistry* metrics{nullptr};
  // Default classification when probing returns kUnknown.
  DiskKind unknownFallbackKind{DiskKind::kHdd};
  // Whether to remove spill files when ProcessSpillService is destroyed.
  bool cleanupOnDestroy{true};
  // Active disk probe duration for each spill directory. Zero disables active
  // probing and uses the configured forced/fallback kind.
  std::chrono::milliseconds diskProbeDuration{std::chrono::seconds(1)};
  // Probe geometry and classification policy. spillDir, forcedKind,
  // unknownFallbackKind, and diskProbeDuration remain the authoritative
  // process-level fields and are copied into this probe config at startup.
  DiskProbeConfig diskProbe;
  // Process-wide disk I/O scheduler policy used by the spill store.
  DiskIoConfig diskIo;
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

  // Tests only: tears down the singleton so the next ConfigureDefault can
  // succeed. Safe to call when no Instance has been created.
  static void ResetForTesting();

  // Tests only: creates an isolated, non-singleton service. This is useful
  // for cases that need a different process-level configuration than the
  // singleton used by other tests.
  static std::unique_ptr<ProcessSpillService> CreateForTesting(
      ProcessSpillServiceConfig config);

  EvictResult SubmitSpill(EvictionNode node) override;

  SpillExecutionMode ExecutionMode() const {
    return config_.executionMode;
  }

  bool WaitForProgress(
      ByteCount bytesNeeded,
      std::chrono::milliseconds timeout) override;

  // Async workers exchange only immutable bytes plus a block id token with
  // BufferManager. They must not own or mutate BlockHandle instances.
  struct SpillRequest {
    std::weak_ptr<SpillOwnerToken> owner;
    uint64_t blockId{0};
    uint64_t evictionSequence{0};
    MemoryTag tag{MemoryTag::kInternal};
    std::unique_ptr<AccountedMemory> memory;
  };

  struct SpillCompletion {
    std::weak_ptr<SpillOwnerToken> owner;
    uint64_t blockId{0};
    uint64_t evictionSequence{0};
    MemoryTag tag{MemoryTag::kInternal};
    std::unique_ptr<AccountedMemory> memory;
    SpillLocation location;
    std::string error;
  };

  struct PrefetchRequest {
    std::weak_ptr<SpillOwnerToken> owner;
    uint64_t blockId{0};
    uint64_t evictionSequence{0};
    MemoryTag tag{MemoryTag::kInternal};
    std::unique_ptr<AccountedMemory> memory;
    SpillLocation location;
  };

  struct PrefetchCompletion {
    std::weak_ptr<SpillOwnerToken> owner;
    uint64_t blockId{0};
    uint64_t evictionSequence{0};
    MemoryTag tag{MemoryTag::kInternal};
    std::unique_ptr<AccountedMemory> memory;
    std::string error;
  };

  std::vector<SpillCompletion> DrainCompletions(
      const std::shared_ptr<SpillOwnerToken>& owner);
  std::vector<PrefetchCompletion> DrainPrefetchCompletions(
      const std::shared_ptr<SpillOwnerToken>& owner);

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
  friend class BufferManager;

  // Installs the configuration used by lazy initialization. Exposed only to
  // BufferManager so process service setup has a single public entry point.
  static void ConfigureDefault(ProcessSpillServiceConfig config);
  bool HasPendingSpills(const std::shared_ptr<SpillOwnerToken>& owner) const;

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
  void ExecuteSpill(SpillRequest request);
  EvictResult SubmitPrefetch(PrefetchRequest request);
  void ExecutePrefetch(PrefetchRequest request);

  // Best-effort cleanup of <root>/bolt_spill_<pid>_* directories whose pid
  // is no longer alive. Called once per process at construction time. Any
  // I/O failure is logged but not propagated.
  void CleanupStaleDirsAtStartup();

  ProcessSpillServiceConfig config_;
  MetricsRegistry& metrics_;
  Counter& submitCounter_;
  Counter& scheduledCounter_;
  Counter& backpressuredCounter_;
  Counter& skippedCounter_;
  Counter& failedCounter_;
  Counter& executedCounter_;
  Counter& freedBytesCounter_;
  Gauge& queueDepthGauge_;
  Histogram& submitDuration_;
  Histogram& executeDuration_;
  Histogram& waitForProgressDuration_;
  std::unique_ptr<SpillStore> store_;
  std::atomic<ByteCount> usedDiskBytes_{0};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::condition_variable progressCv_;
  std::deque<SpillRequest> ready_;
  std::deque<PrefetchRequest> prefetchReady_;
  std::deque<SpillCompletion> completed_;
  std::deque<PrefetchCompletion> prefetchCompleted_;
  std::map<std::weak_ptr<SpillOwnerToken>,
           size_t,
           std::owner_less<std::weak_ptr<SpillOwnerToken>>>
      activeByOwner_;
  bool stopping_{false};
  bool started_{false};
  uint64_t progressEpoch_{0};
  std::vector<std::thread> workers_;
};

} // namespace bytedance::bolt::memory::bm
