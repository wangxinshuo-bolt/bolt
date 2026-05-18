/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_set>

#include "bolt/common/memory/bm/Types.h"

namespace bytedance::bolt::memory::bm {

class SpillStore;

// Concrete address of one spilled payload. The medium kind is captured at
// write time so reload paths can attribute Read latency to the right medium
// without re-probing (per design doc §10.2).
//
// SpillLocation is a value type. After successful Write, callers MUST keep
// a copy alive until they call Release; the path string is the canonical
// identity used by SpillStore's bookkeeping. Default-constructed locations
// are "invalid" -- Valid() returns false and they are no-ops on Release.
struct SpillLocation {
  // Filesystem path of the spill file. Empty iff this is an invalid
  // (default-constructed) location.
  std::string path;
  // Logical (uncompressed) bytes the file represents. This is what the
  // caller has to hand back to Read's destination buffer.
  ByteCount logicalBytes{0};
  // Bytes actually stored on disk. Equal to logicalBytes when the
  // payload was written uncompressed (current MVP).
  ByteCount storedBytes{0};
  // Reserved for future compressed spill format: 0 == uncompressed.
  uint8_t compressionCodec{0};
  // Medium snapshot at write time (used for metrics labeling on Read).
  MediumKind medium{MediumKind::kUnknown};

  // Returns true if this location refers to a real spill file.
  // Default-constructed and moved-from locations return false.
  bool Valid() const {
    return !path.empty();
  }
};

// Configuration for one spill directory. Multiple SpillStores share the same
// process; each gets its own SpillStoreConfig.
struct SpillStoreConfig {
  // Filesystem path the store owns. Created on construction if missing.
  // SpillStore is the sole writer; the directory should not be shared.
  std::string spillDir;
  // When true, the destructor removes any live spill files this store
  // produced. Set to false in tests that want to inspect the on-disk
  // artifacts after destruction.
  bool cleanupOnDestroy{true};
  // Forces the medium classification when probing is unavailable or
  // intentionally overridden. Always wins over probing (design doc §10.2).
  MediumKind forcedKind{MediumKind::kUnknown};
  // Fallback medium when probing is inconclusive. Only consulted when
  // forcedKind == kUnknown. Defaults to HDD as the conservative latency
  // assumption.
  MediumKind unknownFallbackKind{MediumKind::kHdd};
};

// RAII handle for a single spill write attempt. Per design doc §10.1 a write
// session is sticky to a single directory, must be closed by exactly one
// successful Write() or its destructor (which never throws and never performs
// I/O), and never returns a partially written location.
//
// MVP: at most one Write() per session; subsequent calls throw. Future
// versions may allow appending multiple chunks before producing a single
// SpillLocation.
class SpillWriteSession {
 public:
  // Sessions are non-copyable.
  SpillWriteSession(SpillWriteSession&& other) noexcept;
  SpillWriteSession& operator=(SpillWriteSession&& other) noexcept;
  SpillWriteSession(const SpillWriteSession&) = delete;
  SpillWriteSession& operator=(const SpillWriteSession&) = delete;

  // If Write() never ran (or threw), the session destructor is a no-op:
  // any partially written file was already cleaned up inline. Never throws.
  ~SpillWriteSession() noexcept;

  // Writes 'bytes' from 'src' into a fresh file under the session's
  // directory and returns a SpillLocation describing it. May only be
  // called once per session.
  // Throws BoltUserError if:
  //   * the session has already been consumed
  //   * the underlying SpillStore was moved-from
  //   * 'src' is null or 'bytes' < 0
  //   * the file cannot be opened or written.
  // On any failure the partial file is removed before the exception
  // propagates so the caller never observes a leaked artifact.
  SpillLocation Write(MemoryTag tag, ConstDataPtr src, ByteCount bytes);

 private:
  friend class SpillStore;
  SpillWriteSession(SpillStore* store, MediumKind medium);

  SpillStore* store_{nullptr};
  MediumKind medium_{MediumKind::kUnknown};
  bool consumed_{false};
};

// Minimal file-backed spill store for BufferManager-owned immutable blocks.
// It intentionally does not reuse exec spill files yet; keeping the format
// private lets us evolve BufferManager without perturbing existing operators.
//
// Threading: Read/Write/Release are safe to call concurrently. The store's
// internal bookkeeping uses a mutex; on-disk I/O happens outside the lock.
class SpillStore {
 public:
  // Creates the spill directory (mkdir -p style) and prepares file
  // bookkeeping. Probes the medium once during construction.
  // Throws std::filesystem_error if the directory cannot be created.
  SpillStore(SpillStoreConfig config, MetricsRegistry* metrics = nullptr);

  // Removes live spill files when cleanupOnDestroy is enabled. Failures to
  // remove individual files are logged but never thrown -- the destructor
  // is noexcept-safe by contract.
  ~SpillStore();

  // Begins a write session sticky to the configured spill directory.
  // 'allowCompression' is reserved for future use; the MVP ignores it.
  // The returned session must be consumed before the SpillStore destructor
  // runs.
  SpillWriteSession BeginWriteAttempt(MemoryTag tag, bool allowCompression);

  // Convenience helper that opens, writes, and finalizes a session in one
  // call. Equivalent to BeginWriteAttempt(tag, false).Write(tag, data, bytes).
  // Same exception contract as SpillWriteSession::Write.
  SpillLocation Write(MemoryTag tag, ConstDataPtr data, ByteCount bytes);

  // Reads a previously written spill file into 'dst'. 'dstCapacity' must be
  // >= location.logicalBytes; the entire payload is read in one shot.
  // Throws BoltUserError on:
  //   * invalid location (Valid()==false)
  //   * null 'dst'
  //   * dstCapacity < location.logicalBytes
  //   * file open failure or short read.
  // The location's reference count on disk is unchanged (no implicit Release).
  void Read(const SpillLocation& location, DataPtr dst, ByteCount dstCapacity);

  // Releases a spill file:
  //   * Valid live path  -> removes the file and forgets it.
  //   * Already released  -> idempotent; bumps doubleReleaseCounter and
  //                          returns without throwing (design doc §10.3).
  //   * Unknown path      -> bumps invalidReleaseCounter and throws
  //                          BoltUserError to surface lifecycle bugs.
  //   * Invalid location  -> no-op.
  void Release(const SpillLocation& location);

  // Returns the medium effective for this store after construction-time
  // probing and config overrides. Stable for the store's lifetime.
  MediumKind Medium() const {
    return medium_;
  }

  // Best-effort cleanup of stale BufferManager-owned spill files under
  // 'cfg.spillDir'. Only removes files whose names match the BM-generated
  // 'bm_*' pattern; other files are left untouched. Safe to call before
  // any SpillStore is constructed (and before/at process startup).
  static void CleanupAtStartup(const SpillStoreConfig& cfg);

 private:
  friend class SpillWriteSession;

  // Allocates a new on-disk path under spillDir for tag-tagged file.
  // Caller must Register/Forget the returned path in lockstep with I/O.
  std::string MakePath(MemoryTag tag);

  // Records 'path' as live so Release() / cleanup can find it later.
  // Idempotent: re-registering an existing path is harmless.
  void RegisterLiveFile(const std::string& path);

  // Removes 'path' from the live set. Returns true iff it was present.
  // Used by SpillWriteSession to roll back partially written files.
  bool ForgetLiveFile(const std::string& path) noexcept;

  const SpillStoreConfig config_;
  MetricsRegistry& metrics_;
  Counter& bytesWrittenCounter_;
  Counter& bytesReadCounter_;
  Counter& doubleReleaseCounter_;
  Counter& invalidReleaseCounter_;
  const MediumKind medium_;
  mutable std::mutex mutex_;
  std::atomic<uint64_t> nextFileId_{0};
  std::unordered_set<std::string> liveFiles_;
  // Paths that have been successfully released. Used to distinguish double
  // release (idempotent + counter) from invalid releases (throw) per design
  // doc §10.3.
  std::unordered_set<std::string> releasedFiles_;
};

} // namespace bytedance::bolt::memory::bm
