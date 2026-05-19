/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bolt/common/memory/bm/GlobalSpillScheduler.h"
#include "bolt/common/memory/bm/SpillClient.h"
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
  // Spill directories. At least one must be provided.
  std::vector<SpillDirConfig> dirs;
  // 0 means unlimited; Write() that would exceed this aggregate limit
  // fails fast with BoltUserError ("disk quota exceeded").
  ByteCount processDiskQuotaBytes{0};
  // Worker thread pool size. 0 means "no workers" (every spill submission
  // is reported as kBackpressured so the caller falls back to synchronous
  // SpillToDisk). Production callers should set this explicitly.
  uint32_t workerThreadCount{0};
  // Hard ceiling on parallel spill tasks running simultaneously.
  uint32_t maxActiveAttempts{16};
  // Optional metrics sink shared by every component the service owns.
  MetricsRegistry* metrics{nullptr};
  // Default classification when probing returns kUnknown.
  DiskKind unknownFallbackKind{DiskKind::kHdd};
  // Whether to remove spill files when ProcessSpillService is destroyed.
  bool cleanupOnDestroy{true};
  // Active disk probe duration for each spill directory. Zero disables active
  // probing and uses the configured forced/fallback kind.
  std::chrono::milliseconds diskProbeDuration{std::chrono::seconds(1)};
};

// Single global spill service (design doc §11/§14). Owns the worker pool,
// file system view, and fairness state.
//
// Lifecycle:
//   1) ConfigureDefault(cfg)   -> mandatory; throws on second call
//   2) Instance()              -> lazy init; throws if not configured
//   3) CreateClient(cfg)       -> BufferManager allocates a SpillClient
//   4) ResetForTesting()       -> tests only; throws if any client alive
//
// Shutdown is explicit: the destructor stops workers, syncs files, and
// best-effort cleans owned spill dirs. atexit is intentionally NOT used
// to avoid static destruction-order surprises.
//
// Thread-safety: all public methods are safe to call concurrently.
class ProcessSpillService {
 public:
  // Returns the (lazily initialized) singleton. The first call after
  // ConfigureDefault constructs the service; subsequent calls return the
  // same reference. Throws BoltUserError if ConfigureDefault has not been
  // called.
  static ProcessSpillService& Instance();

  // Installs the configuration used by lazy initialization. Must be called
  // exactly once per process (including across ResetForTesting cycles in
  // tests). Validates: 'config.dirs' must be non-empty.
  // Throws BoltUserError on:
  //   * a second invocation while an Instance is alive
  //   * an empty 'dirs' vector.
  static void ConfigureDefault(ProcessSpillServiceConfig config);

  // Tests only: tears down the singleton so the next ConfigureDefault can
  // succeed. Throws BoltUserError if any SpillClient created from this
  // service is still alive, surfacing lifecycle bugs early. Safe to call
  // when no Instance has been created.
  static void ResetForTesting();

  // Tests only: creates an isolated, non-singleton service. This is useful
  // for cases that need a different process-level configuration than the
  // singleton used by other tests.
  static std::unique_ptr<ProcessSpillService> CreateForTesting(
      ProcessSpillServiceConfig config);

  // Allocates a new SpillClient bound to this service. The returned
  // shared_ptr keeps the client alive even after BufferManager teardown
  // so any in-flight spill workers can finish their I/O. Tenants may
  // share a tenantId; the service does not deduplicate.
  // Throws BoltUserError if the service is shutting down.
  std::shared_ptr<SpillClient> CreateClient(SpillClientConfig config);

  // Returns the global scheduler. Public for diagnostics and metrics
  // probing only; production code should go through SpillClient.
  GlobalSpillScheduler& Scheduler() {
    return *scheduler_;
  }

  // Snapshot of total disk bytes currently held across all clients. Updated
  // atomically by SpillClient::Write/Release; safe to read from any thread.
  ByteCount UsedDiskBytes() const {
    return usedDiskBytes_.load(std::memory_order_relaxed);
  }

  // The configured process-wide disk quota (0 means unlimited).
  ByteCount ProcessDiskQuotaBytes() const {
    return config_.processDiskQuotaBytes;
  }

  ProcessSpillService(const ProcessSpillService&) = delete;
  ProcessSpillService& operator=(const ProcessSpillService&) = delete;

 private:
  friend class SpillClient;
  // unique_ptr<ProcessSpillService> in the singleton holder needs to invoke
  // our destructor; expose it via std::default_delete without leaking the
  // dtor into the public API.
  friend struct std::default_delete<ProcessSpillService>;

  // Use Instance() / ResetForTesting() / ConfigureDefault().
  explicit ProcessSpillService(ProcessSpillServiceConfig config);
  ~ProcessSpillService();

  // Picks one of the configured stores for a new write. Round-robin via
  // 'storeRobinCursor_'. Caller does not need to hold any lock.
  SpillStore& PickStore();

  // Picks a store for a new write and returns its stable index together with
  // the store reference so SpillLocation can route future Read/Release calls
  // back to the same store.
  std::pair<uint64_t, SpillStore&> PickStoreForWrite();

  // Returns the store that owns 'location'. Throws BoltUserError if the
  // location was not produced by this service or the store index is invalid.
  SpillStore& StoreFor(const SpillLocation& location);

  // Reserves 'bytes' against the process and per-client quotas. Both
  // counters are bumped atomically; on overflow neither is changed and
  // a BoltUserError is thrown ("disk quota exceeded"). Used by
  // SpillClient::Write before issuing the actual file write.
  void ChargeQuota(SpillClient& client, ByteCount bytes);

  // Releases 'bytes' from the process and client quotas. Must mirror a
  // successful prior ChargeQuota; releases beyond the charged amount are
  // clamped at zero. Never throws.
  void CreditQuota(SpillClient& client, ByteCount bytes) noexcept;

  // Best-effort cleanup of <root>/bolt_spill_<pid>_* directories whose pid
  // is no longer alive. Called once per process at construction time. Any
  // I/O failure is logged but not propagated.
  void CleanupStaleDirsAtStartup();

  // Drops 'client' from the live tracker. Called by SpillClient's dtor.
  // Unknown ids are ignored. Never throws.
  void UnregisterClient(uint64_t clientId);

  ProcessSpillServiceConfig config_;
  MetricsRegistry& metrics_;
  std::vector<std::unique_ptr<SpillStore>> stores_;
  std::unique_ptr<GlobalSpillScheduler> scheduler_;
  std::atomic<uint64_t> storeRobinCursor_{0};
  std::atomic<ByteCount> usedDiskBytes_{0};

  mutable std::mutex clientsMutex_;
  // Live client tracker: ResetForTesting() refuses to drop the singleton
  // while any entry is non-zero.
  std::unordered_map<uint64_t, std::weak_ptr<SpillClient>> clients_;
};

} // namespace bytedance::bolt::memory::bm
