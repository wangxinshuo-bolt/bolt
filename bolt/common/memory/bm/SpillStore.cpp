/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillStore.h"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <memory>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"

namespace bytedance::bolt::memory::bm {
namespace {

MetricsRegistry& effectiveRegistry(MetricsRegistry* metrics) {
  return metrics == nullptr ? NoOpMetricsRegistry() : *metrics;
}

class ScopedFd {
 public:
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  int get() const {
    return fd_;
  }

 private:
  int fd_{-1};
};

} // namespace

SpillWriteSession::SpillWriteSession(SpillStore* store, DiskKind disk)
    : store_(store), disk_(disk) {}

SpillWriteSession::SpillWriteSession(SpillWriteSession&& other) noexcept
    : store_(other.store_),
      disk_(other.disk_),
      consumed_(other.consumed_) {
  other.store_ = nullptr;
  other.consumed_ = true;
}

SpillWriteSession& SpillWriteSession::operator=(
    SpillWriteSession&& other) noexcept {
  if (this != &other) {
    store_ = other.store_;
    disk_ = other.disk_;
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
    ScopedFd fd(::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644));
    BOLT_USER_CHECK_GE(
        fd.get(), 0, "Failed to open spill file {} for write", path);

    DiskIoRequest write;
    write.op = DiskIoOp::kWrite;
    write.priority = DiskIoPriority::kLow;
    write.fd = fd.get();
    write.buffer = const_cast<DataPtr>(src);
    write.size = bytes;
    write.offset = 0;
    auto writeDone = store_->ioScheduler_->SubmitAndWait(write);
    BOLT_USER_CHECK_GE(
        writeDone.result,
        0,
        "Failed to write spill file {}: errno {}",
        path,
        -writeDone.result);
    BOLT_USER_CHECK_EQ(
        writeDone.result,
        bytes,
        "Short write to spill file {}: wrote {}, expected {}",
        path,
        writeDone.result,
        bytes);
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
                     << " disk=" << ToString(disk_);
  return SpillLocation{std::move(path), bytes, bytes, 0, disk_};
}

SpillStore::SpillStore(
    SpillStoreConfig config,
    MetricsRegistry* metrics,
    DiskIoScheduler* ioScheduler)
    : config_(std::move(config)),
      metrics_(effectiveRegistry(metrics)),
      ioScheduler_(
          ioScheduler == nullptr ? &ProcessDiskIoService::Instance().Scheduler()
                                 : ioScheduler),
      diskProbe_(config_.diskProbe.kind == DiskKind::kUnknown
                     ? DiskProbeResult{
                           config_.unknownFallbackKind,
                           0,
                           0,
                           TargetP95LatencyForDisk(config_.unknownFallbackKind),
                           false}
                     : config_.diskProbe),
      bytesWrittenCounter_(metrics_.GetCounter(
          "bm_spill_bytes_written",
          fmt::format("disk={}", ToString(diskProbe_.kind)))),
      bytesReadCounter_(metrics_.GetCounter(
          "bm_spill_bytes_read",
          fmt::format("disk={}", ToString(diskProbe_.kind)))),
      doubleReleaseCounter_(metrics_.GetCounter(
          "bm_spill_double_release",
          fmt::format("disk={}", ToString(diskProbe_.kind)))),
      invalidReleaseCounter_(metrics_.GetCounter(
          "bm_spill_invalid_release",
          fmt::format("disk={}", ToString(diskProbe_.kind)))),
      disk_(diskProbe_.kind) {
  std::filesystem::create_directories(config_.spillDir);
  BOLT_MEM_LOG(INFO) << "BufferManager SpillStore created at "
                     << config_.spillDir
                     << " disk=" << ToString(disk_)
                     << " write_iops=" << diskProbe_.writeIops
                     << " read_iops=" << diskProbe_.readIops
                     << " active_probe=" << diskProbe_.activeProbeRan
                     << " direct_io=" << diskProbe_.directIoUsed;
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
  return SpillWriteSession(this, disk_);
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
                     << " disk=" << ToString(location.disk);
  ScopedFd fd(::open(location.path.c_str(), O_RDONLY));
  BOLT_USER_CHECK_GE(
      fd.get(), 0, "Failed to open spill file {} for read", location.path);

  DiskIoRequest read;
  read.op = DiskIoOp::kRead;
  read.priority = DiskIoPriority::kHigh;
  read.fd = fd.get();
  read.buffer = dst;
  read.size = location.logicalBytes;
  read.offset = 0;
  auto readDone = ioScheduler_->SubmitAndWait(read);
  BOLT_USER_CHECK_GE(
      readDone.result,
      0,
      "Failed to read spill file {}: errno {}",
      location.path,
      -readDone.result);
  BOLT_USER_CHECK_EQ(
      readDone.result,
      location.logicalBytes,
      "Short read from spill file {}: got {}, expected {}",
      location.path,
      readDone.result,
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
