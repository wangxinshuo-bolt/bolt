/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <memory>

#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/Types.h"

namespace bytedance::bolt::memory::bm {

class ProcessSpillService;

// Per-tenant view of ProcessSpillService. BufferManager holds one (via
// shared_ptr because workers may need to keep it alive past BM teardown).
// Implements SpillRequester so it can be plugged into BlockEvictor.
//
// Disk-quota enforcement is strict: Write() throws BoltUserError on quota
// excess (process or client). Callers must not catch the exception and
// fall back to compress-only -- the service is the single source of truth
// for spillability.
//
// Lifetime: owned by BufferManager via shared_ptr. The destructor
// unregisters from the scheduler and the service. Spawning a SpillClient
// with disable spill (config.enableSpill==false) skips all registration
// and turns SubmitSpill into a no-op that always returns kFailed.
class SpillClient : public SpillRequester,
                    public std::enable_shared_from_this<SpillClient> {
 public:
  // Releases pending nodes, unregisters from the scheduler, and notifies
  // ProcessSpillService that this client is gone. Never throws.
  ~SpillClient() override;

  SpillClient(const SpillClient&) = delete;
  SpillClient& operator=(const SpillClient&) = delete;

  // Submits 'node' (whose policy must be a spill policy) to the global
  // scheduler. Returns:
  //   kScheduled       -- queued; the worker pool will run it later.
  //   kBackpressured   -- scheduler has too many active attempts or
  //                        workerThreadCount==0; caller should back off.
  //   kSkipped         -- spill is disabled for this client.
  //   kFailed          -- the scheduler is shut down or unreachable.
  EvictResult SubmitSpill(EvictionNode node) override;

  // Blocks until the scheduler reports forward progress or 'timeout'
  // elapses. See Evictor::WaitForProgress for the full contract. Returns
  // false immediately when spill is disabled.
  bool WaitForProgress(
      ByteCount bytesNeeded,
      std::chrono::milliseconds timeout) override;

  // Stable client id assigned by ProcessSpillService at registration.
  // Stable for the client's lifetime; 0 only when spill is disabled.
  uint64_t Id() const {
    return clientId_;
  }

  // Returns the configuration captured at construction. Read-only after
  // construction; safe to call from any thread.
  const SpillClientConfig& Config() const {
    return config_;
  }

  // Persists 'bytes' from 'src' to one of the service's stores. Round-robin
  // across configured directories. Throws BoltUserError on disk-quota
  // exhaustion at either the process or client level. Throws on I/O
  // failure with no on-disk artifact left behind. Bytes are charged to
  // both the client's and the process's used-disk counters atomically.
  SpillLocation Write(MemoryTag tag, ConstDataPtr src, ByteCount bytes);

  // Rehydrates a previously written payload via the service's store. Throws
  // BoltUserError if 'location' was not produced by this client (or by a
  // sibling client of the same service). 'dstCapacity' must be >=
  // location.logicalBytes.
  void Read(const SpillLocation& location, DataPtr dst, ByteCount dstCapacity);

  // Releases a spill file and credits the process and client quotas.
  // Idempotent: releasing the same location twice is safe (the second
  // call is reported but does not throw). Never throws.
  void Release(const SpillLocation& location) noexcept;

  // Disk bytes currently attributed to this client. Updated atomically by
  // Write/Release; safe to read from any thread.
  ByteCount UsedDiskBytes() const {
    return usedDiskBytes_.load(std::memory_order_relaxed);
  }

 private:
  friend class ProcessSpillService;
  SpillClient(
      ProcessSpillService* service,
      uint64_t clientId,
      SpillClientConfig config);

  ProcessSpillService* service_{nullptr};
  const uint64_t clientId_{0};
  const SpillClientConfig config_;
  std::atomic<ByteCount> usedDiskBytes_{0};
};

} // namespace bytedance::bolt::memory::bm
