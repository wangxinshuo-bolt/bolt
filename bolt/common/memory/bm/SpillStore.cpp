/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillStore.h"

#include <filesystem>
#include <fstream>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"

namespace bytedance::bolt::memory::bm {
namespace {

// Best-effort medium probe based on the configured directory. The MVP only
// inspects the path; real implementations will read /sys/block + statfs and
// fall back to config.unknownFallbackKind. Per design doc §10.2 forced kinds
// always win over probing.
MediumKind probeMedium(const SpillStoreConfig& config) {
  if (config.forcedKind != MediumKind::kUnknown) {
    return config.forcedKind;
  }
  // No real probing yet: return the user-supplied fallback.
  return config.unknownFallbackKind;
}

MetricsRegistry& effectiveRegistry(MetricsRegistry* metrics) {
  return metrics == nullptr ? NoOpMetricsRegistry() : *metrics;
}

} // namespace

SpillWriteSession::SpillWriteSession(SpillStore* store, MediumKind medium)
    : store_(store), medium_(medium) {}

SpillWriteSession::SpillWriteSession(SpillWriteSession&& other) noexcept
    : store_(other.store_),
      medium_(other.medium_),
      consumed_(other.consumed_) {
  other.store_ = nullptr;
  other.consumed_ = true;
}

SpillWriteSession& SpillWriteSession::operator=(
    SpillWriteSession&& other) noexcept {
  if (this != &other) {
    store_ = other.store_;
    medium_ = other.medium_;
    consumed_ = other.consumed_;
    other.store_ = nullptr;
    other.consumed_ = true;
  }
  return *this;
}

SpillWriteSession::~SpillWriteSession() noexcept {
  // Per design doc §10.1, the destructor never performs I/O and never throws.
  // Nothing to do: Write() either committed (consumed_=true) or already
  // cleaned up its partial file via SpillStore::ForgetLiveFile.
}

SpillLocation SpillWriteSession::Write(
    MemoryTag tag,
    ConstDataPtr src,
    ByteCount bytes) {
  BOLT_USER_CHECK_NOT_NULL(store_, "SpillWriteSession has no associated store");
  BOLT_USER_CHECK(!consumed_, "SpillWriteSession::Write may be called once");
  BOLT_USER_CHECK_NOT_NULL(src, "Cannot spill a null buffer");
  consumed_ = true;

  std::string path = store_->MakePath(tag);
  store_->RegisterLiveFile(path);

  try {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    BOLT_USER_CHECK(out.good(), "Failed to open spill file {} for write", path);
    out.write(reinterpret_cast<const char*>(src), bytes);
    out.close();
    BOLT_USER_CHECK(out.good(), "Failed to write spill file {}", path);
  } catch (...) {
    // Roll back the live-file registration and remove the partial file so the
    // session destructor never has to do I/O.
    store_->ForgetLiveFile(path);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    throw;
  }

  store_->bytesWrittenCounter_.Add(bytes);
  BOLT_MEM_LOG(INFO) << "BufferManager SpillStore wrote " << bytes
                     << " bytes to " << path << " tag=" << ToString(tag)
                     << " medium=" << ToString(medium_);
  return SpillLocation{std::move(path), bytes, bytes, 0, medium_};
}

SpillStore::SpillStore(SpillStoreConfig config, MetricsRegistry* metrics)
    : config_(std::move(config)),
      metrics_(effectiveRegistry(metrics)),
      bytesWrittenCounter_(metrics_.GetCounter(
          "bm_spill_bytes_written",
          fmt::format("medium={}", ToString(probeMedium(config_))))),
      bytesReadCounter_(metrics_.GetCounter(
          "bm_spill_bytes_read",
          fmt::format("medium={}", ToString(probeMedium(config_))))),
      doubleReleaseCounter_(metrics_.GetCounter(
          "bm_spill_double_release",
          fmt::format("medium={}", ToString(probeMedium(config_))))),
      invalidReleaseCounter_(metrics_.GetCounter(
          "bm_spill_invalid_release",
          fmt::format("medium={}", ToString(probeMedium(config_))))),
      medium_(probeMedium(config_)) {
  std::filesystem::create_directories(config_.spillDir);
  BOLT_MEM_LOG(INFO) << "BufferManager SpillStore created at "
                     << config_.spillDir
                     << " medium=" << ToString(medium_);
}

SpillStore::~SpillStore() {
  if (!config_.cleanupOnDestroy) {
    return;
  }
  std::vector<std::string> files;
  {
    std::lock_guard<std::mutex> l(mutex_);
    files.assign(liveFiles_.begin(), liveFiles_.end());
    liveFiles_.clear();
    releasedFiles_.clear();
  }
  for (const auto& file : files) {
    std::error_code ec;
    std::filesystem::remove(file, ec);
    if (ec) {
      BOLT_MEM_LOG(WARNING) << "Failed to cleanup BufferManager spill file "
                            << file << ": " << ec.message();
    }
  }
}

SpillWriteSession SpillStore::BeginWriteAttempt(
    MemoryTag /*tag*/,
    bool /*allowCompression*/) {
  return SpillWriteSession(this, medium_);
}

SpillLocation SpillStore::Write(
    MemoryTag tag,
    ConstDataPtr data,
    ByteCount bytes) {
  // Convenience wrapper that mirrors the legacy single-shot API used by
  // BlockHandle. Internally still goes through a SpillWriteSession so writers
  // pick up RAII semantics for free.
  SpillWriteSession session = BeginWriteAttempt(tag, /*allowCompression=*/false);
  return session.Write(tag, data, bytes);
}

void SpillStore::Read(
    const SpillLocation& location,
    DataPtr dst,
    ByteCount dstCapacity) {
  BOLT_USER_CHECK(location.Valid(), "Invalid spill location");
  BOLT_USER_CHECK_NOT_NULL(dst, "Cannot read spill into a null buffer");
  BOLT_USER_CHECK_GE(
      dstCapacity,
      location.logicalBytes,
      "Spill read buffer too small: capacity {}, needed {}",
      dstCapacity,
      location.logicalBytes);

  BOLT_MEM_LOG(INFO) << "BufferManager SpillStore reading "
                     << location.logicalBytes << " bytes from "
                     << location.path
                     << " medium=" << ToString(location.medium);
  std::ifstream in(location.path, std::ios::binary);
  BOLT_USER_CHECK(
      in.good(), "Failed to open spill file {} for read", location.path);
  in.read(reinterpret_cast<char*>(dst), location.logicalBytes);
  BOLT_USER_CHECK(
      static_cast<ByteCount>(in.gcount()) == location.logicalBytes,
      "Short read from spill file {}: got {}, expected {}",
      location.path,
      in.gcount(),
      location.logicalBytes);
  bytesReadCounter_.Add(location.logicalBytes);
}

void SpillStore::Release(const SpillLocation& location) {
  if (!location.Valid()) {
    return;
  }
  bool live = false;
  bool alreadyReleased = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (liveFiles_.erase(location.path) != 0) {
      live = true;
      releasedFiles_.insert(location.path);
    } else if (releasedFiles_.count(location.path) != 0) {
      alreadyReleased = true;
    }
  }
  if (alreadyReleased) {
    // Idempotent: design doc §10.3 says callers may release the same location
    // more than once during recovery. Count it for visibility but don't throw.
    doubleReleaseCounter_.Add(1);
    BOLT_MEM_LOG(WARNING)
        << "BufferManager SpillStore double release of " << location.path;
    return;
  }
  if (!live) {
    invalidReleaseCounter_.Add(1);
    BOLT_USER_FAIL(
        "BufferManager SpillStore release of unknown path {}", location.path);
  }
  std::error_code ec;
  std::filesystem::remove(location.path, ec);
  if (ec) {
    BOLT_MEM_LOG(WARNING) << "Failed to remove BufferManager spill file "
                          << location.path << ": " << ec.message();
  } else {
    BOLT_MEM_LOG(INFO) << "BufferManager SpillStore released "
                       << location.path;
  }
}

void SpillStore::CleanupAtStartup(const SpillStoreConfig& cfg) {
  std::error_code ec;
  if (cfg.spillDir.empty() || !std::filesystem::exists(cfg.spillDir, ec)) {
    return;
  }
  // Best-effort: walk the directory and remove any BufferManager-owned spill
  // files. We never recurse into unknown subtrees (design doc §10.4).
  for (const auto& entry :
       std::filesystem::directory_iterator(cfg.spillDir, ec)) {
    if (ec) {
      break;
    }
    if (!entry.is_regular_file(ec)) {
      continue;
    }
    const auto name = entry.path().filename().string();
    if (name.rfind("bm_", 0) != 0) {
      continue;
    }
    std::error_code rmEc;
    std::filesystem::remove(entry.path(), rmEc);
    if (rmEc) {
      BOLT_MEM_LOG(WARNING)
          << "CleanupAtStartup failed to remove " << entry.path().string()
          << ": " << rmEc.message();
    }
  }
}

std::string SpillStore::MakePath(MemoryTag tag) {
  return (std::filesystem::path(config_.spillDir) /
          fmt::format("bm_{}_{}.spill", ToString(tag), nextFileId_++))
      .string();
}

void SpillStore::RegisterLiveFile(const std::string& path) {
  std::lock_guard<std::mutex> l(mutex_);
  liveFiles_.insert(path);
}

bool SpillStore::ForgetLiveFile(const std::string& path) noexcept {
  std::lock_guard<std::mutex> l(mutex_);
  return liveFiles_.erase(path) != 0;
}

} // namespace bytedance::bolt::memory::bm
