/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillFileStore.h"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <memory>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/Observability.h"
#include "bolt/common/memory/bm/SpillCompression.h"

namespace bytedance::bolt::memory::bm {
namespace {

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

SpillWriteSession::SpillWriteSession(SpillFileStore* store, DiskKind disk)
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
  // Nothing to do: Write() either committed or cleaned up partial state.
}

SpillLocation SpillWriteSession::Write(
    MemoryTag tag,
    ConstDataPtr src,
    ByteCount bytes) {
  BOLT_USER_CHECK_NOT_NULL(store_, "SpillWriteSession has no associated store");
  ScopedBmTimer timer(store_->writeDuration_);
  BOLT_USER_CHECK(!consumed_, "SpillWriteSession::Write may be called once");
  BOLT_USER_CHECK_NOT_NULL(src, "Cannot spill a null buffer");
  consumed_ = true;

  const bool compressionAttempted =
      store_->compressionConfig_.enabled &&
      store_->compressionConfig_.codec == SpillCompressionCodec::kZstd &&
      bytes >= store_->compressionConfig_.minBytes;
  auto payload = PrepareSpillPayload(store_->compressionConfig_, src, bytes);
  if (compressionAttempted) {
    store_->compressAttemptCounter_.Add(1);
  }
  if (payload.codec == SpillCompressionCodec::kZstd) {
    store_->compressSavedBytesCounter_.Add(bytes - payload.storedBytes);
  } else if (compressionAttempted) {
    store_->compressFallbackRawCounter_.Add(1);
    BOLT_MEM_VLOG(1) << "BufferManager SpillFileStore compression fallback"
                       << " logical=" << bytes
                       << " stored=" << payload.storedBytes
                       << " minSavingsRatio="
                       << store_->compressionConfig_.minSavingsRatio;
  }
  auto location = store_->smallAllocator_.Allocate(
      bytes, payload.storedBytes, payload.codec, store_->disk_);
  if (!location.Valid()) {
    location = store_->AllocateDedicated(
        tag, bytes, payload.storedBytes, payload.codec);
  } else {
    store_->RegisterLiveFile(location.path);
    std::lock_guard<std::mutex> l(store_->mutex_);
    store_->liveLocations_.insert(store_->LocationKey(location));
  }
  try {
    store_->WriteToLocation(location, payload.data);
  } catch (...) {
    if (location.smallSlot) {
      const bool removeFile = store_->smallAllocator_.Rollback(location);
      {
        std::lock_guard<std::mutex> l(store_->mutex_);
        store_->liveLocations_.erase(store_->LocationKey(location));
      }
      store_->ForgetLiveFile(location.path);
      if (removeFile) {
        std::error_code ec;
        std::filesystem::remove(location.path, ec);
      }
    } else {
      store_->ForgetLiveFile(location.path);
      std::error_code ec;
      std::filesystem::remove(location.path, ec);
    }
    throw;
  }

  store_->bytesWrittenCounter_.Add(bytes);
  store_->bytesStoredCounter_.Add(location.storedBytes);
  if (location.smallSlot) {
    store_->smallSlotCounter_.Add(1);
  } else {
    store_->dedicatedFileCounter_.Add(1);
  }
  BOLT_MEM_VLOG(1) << "BufferManager SpillFileStore wrote " << bytes
      << " bytes to " << location.path
                     << " offset=" << location.offset
                     << " stored=" << location.storedBytes
                     << " slot=" << location.slotBytes
                     << " path_type="
                     << (location.smallSlot ? "small_slot"
                                            : "dedicated_file")
                     << " codec="
                     << static_cast<int>(location.compressionCodec)
                     << " tag=" << ToString(tag)
                     << " disk=" << ToString(disk_);
  return location;
}

SpillFileStore::SpillFileStore(
    SpillFileStoreConfig config,
    MetricsRegistry* metrics,
    DiskIoScheduler* ioScheduler)
    : config_(std::move(config)),
      diskProbe_(config_.diskProbe.kind == DiskKind::kUnknown
                     ? DiskProbeResult{
                           config_.unknownFallbackKind,
                           0,
                           0,
                           false,
                           false}
                     : config_.diskProbe),
      compressionConfig_(config_.compression),
      smallAllocator_(config_.smallSpill, diskProbe_.kind, config_.spillDir),
      metricLabels_(fmt::format("disk={}", ToString(diskProbe_.kind))),
      metrics_(EffectiveMetricsRegistry(metrics)),
      ioScheduler_(
          ioScheduler == nullptr ? &DiskIoTaskExecutor::Instance().Scheduler()
                                 : ioScheduler),
      bytesWrittenCounter_(metrics_.GetCounter(
          "bm_spill_bytes_written",
          metricLabels_)),
      bytesStoredCounter_(metrics_.GetCounter(
          "bm_spill_bytes_stored",
          metricLabels_)),
      bytesReadCounter_(metrics_.GetCounter(
          "bm_spill_bytes_read",
          metricLabels_)),
      releaseCounter_(metrics_.GetCounter(
          "bm_spill_release_total",
          metricLabels_)),
      smallSlotCounter_(metrics_.GetCounter(
          "bm_spill_small_slot_total",
          metricLabels_)),
      dedicatedFileCounter_(metrics_.GetCounter(
          "bm_spill_dedicated_file_total",
          metricLabels_)),
      compressAttemptCounter_(metrics_.GetCounter(
          "bm_spill_compress_attempt_total",
          metricLabels_)),
      compressSavedBytesCounter_(metrics_.GetCounter(
          "bm_spill_compress_saved_bytes",
          metricLabels_)),
      compressFallbackRawCounter_(metrics_.GetCounter(
          "bm_spill_compress_fallback_raw_total",
          metricLabels_)),
      doubleReleaseCounter_(metrics_.GetCounter(
          "bm_spill_double_release",
          metricLabels_)),
      invalidReleaseCounter_(metrics_.GetCounter(
          "bm_spill_invalid_release",
          metricLabels_)),
      writeDuration_(metrics_.GetHistogram(
          "bm_spill_write_duration_us",
          metricLabels_)),
      readDuration_(metrics_.GetHistogram(
          "bm_spill_read_duration_us",
          metricLabels_)),
      releaseDuration_(metrics_.GetHistogram(
          "bm_spill_release_duration_us",
          metricLabels_)),
      disk_(diskProbe_.kind) {
  std::filesystem::create_directories(config_.spillDir);
  const auto& smallConfig = smallAllocator_.Config();
  BOLT_MEM_LOG(INFO) << "BufferManager SpillFileStore created at "
                     << config_.spillDir
                     << " disk=" << ToString(disk_)
                     << " small_spill=" << smallConfig.enabled
                     << " dedicated_threshold="
                     << smallConfig.dedicatedFileThresholdBytes
                     << " slab_file_bytes=" << smallConfig.slabFileBytes
                     << " write_iops=" << diskProbe_.writeIops
                     << " read_iops=" << diskProbe_.readIops
                     << " active_probe=" << diskProbe_.activeProbeRan
                     << " direct_io=" << diskProbe_.directIoUsed
                     << " compression_enabled="
                     << compressionConfig_.enabled
                     << " compression_min_bytes="
                     << compressionConfig_.minBytes
                     << " compression_min_savings_ratio="
                     << compressionConfig_.minSavingsRatio;
}

SpillFileStore::~SpillFileStore() {
  if (!config_.cleanupOnDestroy) {
    return;
  }
  std::vector<std::string> files;
  {
    std::lock_guard<std::mutex> l(mutex_);
    files.assign(liveFiles_.begin(), liveFiles_.end());
    liveFiles_.clear();
    liveLocations_.clear();
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

SpillWriteSession SpillFileStore::BeginWriteAttempt(
    MemoryTag /*tag*/,
    bool /*allowCompression*/) {
  return SpillWriteSession(this, disk_);
}

SpillLocation SpillFileStore::Write(
    MemoryTag tag,
    ConstDataPtr data,
    ByteCount bytes) {
  // Convenience wrapper that mirrors the legacy single-shot API used by
  // BlockHandle. Internally still goes through a SpillWriteSession so writers
  // pick up RAII semantics for free.
  SpillWriteSession session = BeginWriteAttempt(tag, /*allowCompression=*/false);
  return session.Write(tag, data, bytes);
}

void SpillFileStore::Read(
    const SpillLocation& location,
    DataPtr dst,
    ByteCount dstCapacity) {
  ScopedBmTimer timer(readDuration_);
  BOLT_USER_CHECK(location.Valid(), "Invalid spill location");
  BOLT_USER_CHECK_NOT_NULL(dst, "Cannot read spill into a null buffer");
  BOLT_USER_CHECK_GE(
      dstCapacity,
      location.logicalBytes,
      "Spill read buffer too small: capacity {}, needed {}",
      dstCapacity,
      location.logicalBytes);

  BOLT_MEM_VLOG(1) << "BufferManager SpillFileStore reading "
                     << location.logicalBytes << " bytes from "
                     << location.path
                     << " offset=" << location.offset
                     << " stored=" << location.storedBytes
                     << " codec="
                     << static_cast<int>(location.compressionCodec)
                     << " disk=" << ToString(location.disk);
  ScopedFd fd(::open(location.path.c_str(), O_RDONLY));
  BOLT_USER_CHECK_GE(
      fd.get(), 0, "Failed to open spill file {} for read", location.path);

  std::vector<uint8_t> compressed;
  DataPtr readTarget = dst;
  if (location.compressionCodec != SpillCompressionCodec::kNone) {
    compressed.resize(location.storedBytes);
    readTarget = compressed.data();
  }

  DiskIoRequest read;
  read.op = DiskIoOp::kRead;
  read.priority = DiskIoPriority::kHigh;
  read.fd = fd.get();
  read.buffer = readTarget;
  read.size = location.storedBytes;
  read.offset = location.offset;
  auto readDone = ioScheduler_->SubmitAndWait(read);
  BOLT_USER_CHECK_GE(
      readDone.result,
      0,
      "Failed to read spill file {}: errno {}",
      location.path,
      -readDone.result);
  BOLT_USER_CHECK_EQ(
      readDone.result,
      location.storedBytes,
      "Short read from spill file {}: got {}, expected {}",
      location.path,
      readDone.result,
      location.storedBytes);
  if (location.compressionCodec == SpillCompressionCodec::kZstd) {
    DecompressSpillPayload(location, compressed.data(), dst);
  } else {
    BOLT_USER_CHECK_EQ(
        location.storedBytes,
        location.logicalBytes,
        "Uncompressed spill location has mismatched stored/logical bytes");
  }
  bytesReadCounter_.Add(location.logicalBytes);
}

void SpillFileStore::Release(const SpillLocation& location) {
  ScopedBmTimer timer(releaseDuration_);
  if (!location.Valid()) {
    invalidReleaseCounter_.Add(1);
    return;
  }
  bool live = false;
  bool alreadyReleased = false;
  bool removeFile = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    const auto key = LocationKey(location);
    if (liveLocations_.erase(key) != 0) {
      live = true;
      releasedFiles_.insert(key);
    } else if (releasedFiles_.count(key) != 0) {
      alreadyReleased = true;
    }
    if (live && location.smallSlot) {
      removeFile = smallAllocator_.Release(location);
      if (removeFile) {
        liveFiles_.erase(location.path);
      }
    } else if (live) {
      removeFile = liveFiles_.erase(location.path) != 0;
    }
  }
  if (alreadyReleased) {
    doubleReleaseCounter_.Add(1);
    BOLT_MEM_LOG(WARNING)
        << "BufferManager SpillFileStore double release of " << location.path;
    return;
  }
  if (!live) {
    invalidReleaseCounter_.Add(1);
    BOLT_USER_FAIL(
        "BufferManager SpillFileStore release of unknown path {}", location.path);
  }
  releaseCounter_.Add(1);
  if (removeFile) {
    std::error_code ec;
    std::filesystem::remove(location.path, ec);
    if (ec) {
      BOLT_MEM_LOG(WARNING) << "Failed to remove BufferManager spill file "
                            << location.path << ": " << ec.message();
    } else {
      BOLT_MEM_VLOG(1) << "BufferManager SpillFileStore released "
                         << location.path;
    }
  }
}

void SpillFileStore::CleanupAtStartup(const SpillFileStoreConfig& cfg) {
  std::error_code ec;
  if (cfg.spillDir.empty() || !std::filesystem::exists(cfg.spillDir, ec)) {
    return;
  }
  // Best-effort cleanup only touches BufferManager-owned spill files.
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

std::string SpillFileStore::MakeDedicatedPath(MemoryTag tag) {
  return (std::filesystem::path(config_.spillDir) /
          fmt::format("bm_{}_{}.spill", ToString(tag), nextFileId_++))
      .string();
}

std::string SpillFileStore::LocationKey(const SpillLocation& location) const {
  return LocationKey(location.path, location.offset);
}

std::string SpillFileStore::LocationKey(
    const std::string& path,
    uint64_t offset) const {
  return fmt::format("{}:{}", path, offset);
}

SpillLocation SpillFileStore::AllocateDedicated(
    MemoryTag tag,
    ByteCount logicalBytes,
    ByteCount storedBytes,
    SpillCompressionCodec codec) {
  SpillLocation location{
      MakeDedicatedPath(tag),
      0,
      logicalBytes,
      storedBytes,
      0,
      false,
      codec,
      disk_};
  RegisterLiveFile(location.path);
  std::lock_guard<std::mutex> l(mutex_);
  liveLocations_.insert(LocationKey(location));
  return location;
}

void SpillFileStore::WriteToLocation(
    const SpillLocation& location,
    ConstDataPtr src) {
  const auto flags =
      location.smallSlot ? (O_CREAT | O_WRONLY) : (O_CREAT | O_TRUNC | O_WRONLY);
  ScopedFd fd(::open(location.path.c_str(), flags, 0644));
  BOLT_USER_CHECK_GE(
      fd.get(), 0, "Failed to open spill file {} for write", location.path);

  DiskIoRequest write;
  write.op = DiskIoOp::kWrite;
  write.priority = DiskIoPriority::kLow;
  write.fd = fd.get();
  write.buffer = const_cast<DataPtr>(src);
  write.size = location.storedBytes;
  write.offset = location.offset;
  auto writeDone = ioScheduler_->SubmitAndWait(write);
  BOLT_USER_CHECK_GE(
      writeDone.result,
      0,
      "Failed to write spill file {}: errno {}",
      location.path,
      -writeDone.result);
  BOLT_USER_CHECK_EQ(
      writeDone.result,
      location.storedBytes,
      "Short write to spill file {}: wrote {}, expected {}",
      location.path,
      writeDone.result,
      location.storedBytes);
}

void SpillFileStore::RegisterLiveFile(const std::string& path) {
  std::lock_guard<std::mutex> l(mutex_);
  liveFiles_.insert(path);
}

bool SpillFileStore::ForgetLiveFile(const std::string& path) noexcept {
  std::lock_guard<std::mutex> l(mutex_);
  liveLocations_.erase(LocationKey(path, 0));
  return liveFiles_.erase(path) != 0;
}

} // namespace bytedance::bolt::memory::bm
