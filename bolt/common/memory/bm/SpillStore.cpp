/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/SpillStore.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <zstd.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"

namespace bytedance::bolt::memory::bm {
namespace {

MetricsRegistry& effectiveRegistry(MetricsRegistry* metrics) {
  return metrics == nullptr ? NoOpMetricsRegistry() : *metrics;
}

SmallSpillConfig defaultSmallSpillConfig(DiskKind disk) {
  SmallSpillConfig config;
  config.enabled = true;
  config.dedicatedFileThresholdBytes = 4 << 20;
  switch (disk) {
    case DiskKind::kNvme:
      config.slabFileBytes = 256 << 20;
      break;
    case DiskKind::kSsd:
      config.slabFileBytes = 128 << 20;
      break;
    case DiskKind::kHdd:
    case DiskKind::kNetworkFs:
    case DiskKind::kUnknown:
      config.slabFileBytes = 64 << 20;
      break;
  }
  config.sizeClasses = {
      4 << 10,
      8 << 10,
      16 << 10,
      32 << 10,
      64 << 10,
      128 << 10,
      256 << 10,
      512 << 10,
      1 << 20,
      2 << 20,
      4 << 20};
  return config;
}

SmallSpillConfig normalizeSmallSpillConfig(
    SmallSpillConfig config,
    DiskKind disk) {
  auto defaults = defaultSmallSpillConfig(disk);
  if (config.dedicatedFileThresholdBytes == 0) {
    config.dedicatedFileThresholdBytes = defaults.dedicatedFileThresholdBytes;
  }
  if (config.slabFileBytes == 0) {
    config.slabFileBytes = defaults.slabFileBytes;
  }
  if (config.sizeClasses.empty()) {
    config.sizeClasses = std::move(defaults.sizeClasses);
  }
  std::sort(config.sizeClasses.begin(), config.sizeClasses.end());
  config.sizeClasses.erase(
      std::unique(config.sizeClasses.begin(), config.sizeClasses.end()),
      config.sizeClasses.end());
  config.sizeClasses.erase(
      std::remove(config.sizeClasses.begin(), config.sizeClasses.end(), 0),
      config.sizeClasses.end());
  config.sizeClasses.erase(
      std::remove_if(
          config.sizeClasses.begin(),
          config.sizeClasses.end(),
          [&](ByteCount slotBytes) {
            return slotBytes > config.dedicatedFileThresholdBytes;
          }),
      config.sizeClasses.end());
  return config;
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

  auto payload = store_->PreparePayload(src, bytes);
  auto location = store_->AllocateSmallSlot(
      tag, bytes, payload.storedBytes, payload.codec);
  if (!location.Valid()) {
    location = store_->AllocateDedicated(
        tag, bytes, payload.storedBytes, payload.codec);
  }
  try {
    store_->WriteToLocation(location, payload.data);
  } catch (...) {
    if (location.smallSlot) {
      store_->RollbackSmallSlot(location);
    } else {
      store_->ForgetLiveFile(location.path);
      std::error_code ec;
      std::filesystem::remove(location.path, ec);
    }
    throw;
  }

  store_->bytesWrittenCounter_.Add(bytes);
  BOLT_MEM_LOG(INFO) << "BufferManager SpillStore wrote " << bytes
      << " bytes to " << location.path
                     << " offset=" << location.offset
                     << " stored=" << location.storedBytes
                     << " slot=" << location.slotBytes
                     << " codec="
                     << static_cast<int>(location.compressionCodec)
                     << " tag=" << ToString(tag)
                     << " disk=" << ToString(disk_);
  return location;
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
      smallConfig_(normalizeSmallSpillConfig(config_.smallSpill, diskProbe_.kind)),
      compressionConfig_(config_.compression),
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
  if (smallConfig_.enabled) {
    for (const auto slotBytes : smallConfig_.sizeClasses) {
      if (slotBytes == 0 || slotBytes > smallConfig_.dedicatedFileThresholdBytes) {
        continue;
      }
      smallClasses_.push_back(SmallSizeClass{
          slotBytes,
          std::max<ByteCount>(slotBytes, smallConfig_.slabFileBytes),
          {}});
    }
  }
  BOLT_MEM_LOG(INFO) << "BufferManager SpillStore created at "
                     << config_.spillDir
                     << " disk=" << ToString(disk_)
                     << " small_spill=" << smallConfig_.enabled
                     << " dedicated_threshold="
                     << smallConfig_.dedicatedFileThresholdBytes
                     << " slab_file_bytes=" << smallConfig_.slabFileBytes
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
    const auto decompressed = ZSTD_decompress(
        dst,
        static_cast<size_t>(location.logicalBytes),
        compressed.data(),
        static_cast<size_t>(location.storedBytes));
    BOLT_USER_CHECK(
        !ZSTD_isError(decompressed),
        "Failed to decompress spill file {}: {}",
        location.path,
        ZSTD_getErrorName(decompressed));
    BOLT_USER_CHECK_EQ(
        decompressed,
        location.logicalBytes,
        "Decompressed spill size mismatch for {}: got {}, expected {}",
        location.path,
        decompressed,
        location.logicalBytes);
  } else {
    BOLT_USER_CHECK_EQ(
        location.storedBytes,
        location.logicalBytes,
        "Uncompressed spill location has mismatched stored/logical bytes");
  }
  bytesReadCounter_.Add(location.logicalBytes);
}

void SpillStore::Release(const SpillLocation& location) {
  if (!location.Valid()) {
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
      const auto classIndex = SmallClassFor(location.slotBytes);
      if (classIndex.has_value()) {
        const auto slotIndex = location.offset / location.slotBytes;
        auto& cls = smallClasses_[*classIndex];
        for (auto& slab : cls.slabs) {
          if (!slab.deleted && slab.path == location.path) {
            if (slab.usedSlots > 0) {
              --slab.usedSlots;
            }
            slab.freeSlots.push_back(slotIndex);
            if (slab.usedSlots == 0) {
              slab.deleted = true;
              liveFiles_.erase(slab.path);
              removeFile = true;
            }
            break;
          }
        }
      }
    } else if (live) {
      removeFile = liveFiles_.erase(location.path) != 0;
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
  if (removeFile) {
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

std::string SpillStore::MakeDedicatedPath(MemoryTag tag) {
  return (std::filesystem::path(config_.spillDir) /
          fmt::format("bm_{}_{}.spill", ToString(tag), nextFileId_++))
      .string();
}

std::string SpillStore::MakeSmallSlabPath(ByteCount slotBytes) {
  return (std::filesystem::path(config_.spillDir) /
          fmt::format("bm_small_{}_{}.spill", slotBytes, nextSmallSlabId_++))
      .string();
}

std::string SpillStore::LocationKey(const SpillLocation& location) const {
  return LocationKey(location.path, location.offset);
}

std::string SpillStore::LocationKey(
    const std::string& path,
    uint64_t offset) const {
  return fmt::format("{}:{}", path, offset);
}

std::optional<size_t> SpillStore::SmallClassFor(ByteCount bytes) const {
  if (!smallConfig_.enabled || bytes == 0 ||
      bytes > smallConfig_.dedicatedFileThresholdBytes) {
    return std::nullopt;
  }
  for (size_t i = 0; i < smallClasses_.size(); ++i) {
    if (bytes <= smallClasses_[i].slotBytes) {
      return i;
    }
  }
  return std::nullopt;
}

SpillStore::PreparedPayload SpillStore::PreparePayload(
    ConstDataPtr src,
    ByteCount bytes) const {
  PreparedPayload payload;
  payload.data = src;
  payload.storedBytes = bytes;
  if (!compressionConfig_.enabled ||
      compressionConfig_.codec != SpillCompressionCodec::kZstd ||
      bytes < compressionConfig_.minBytes) {
    return payload;
  }

  const auto bound = ZSTD_compressBound(static_cast<size_t>(bytes));
  payload.compressed.resize(bound);
  const auto compressedBytes = ZSTD_compress(
      payload.compressed.data(),
      payload.compressed.size(),
      src,
      static_cast<size_t>(bytes),
      compressionConfig_.level);
  if (ZSTD_isError(compressedBytes)) {
    payload.compressed.clear();
    return payload;
  }
  const auto requiredSavings =
      static_cast<double>(bytes) * compressionConfig_.minSavingsRatio;
  if (compressedBytes >= bytes ||
      static_cast<double>(bytes - compressedBytes) < requiredSavings) {
    payload.compressed.clear();
    return payload;
  }
  payload.compressed.resize(compressedBytes);
  payload.data = payload.compressed.data();
  payload.storedBytes = compressedBytes;
  payload.codec = SpillCompressionCodec::kZstd;
  return payload;
}

SpillLocation SpillStore::AllocateSmallSlot(
    MemoryTag /*tag*/,
    ByteCount logicalBytes,
    ByteCount storedBytes,
    SpillCompressionCodec codec) {
  const auto classIndex = SmallClassFor(storedBytes);
  if (!classIndex.has_value()) {
    return SpillLocation{};
  }

  std::lock_guard<std::mutex> l(mutex_);
  auto& cls = smallClasses_[*classIndex];
  SmallSlabFile* selected{nullptr};
  uint64_t slotIndex = 0;
  for (auto& slab : cls.slabs) {
    if (slab.deleted) {
      continue;
    }
    if (!slab.freeSlots.empty()) {
      selected = &slab;
      slotIndex = slab.freeSlots.front();
      slab.freeSlots.pop_front();
      break;
    }
    if (slab.nextSlot < slab.totalSlots) {
      selected = &slab;
      slotIndex = slab.nextSlot++;
      break;
    }
  }
  if (selected == nullptr) {
    const auto slots =
        std::max<uint64_t>(1, cls.slabFileBytes / cls.slotBytes);
    cls.slabs.push_back(SmallSlabFile{
        MakeSmallSlabPath(cls.slotBytes),
        cls.slotBytes,
        1,
        0,
        slots,
        false,
        {}});
    selected = &cls.slabs.back();
    slotIndex = 0;
    liveFiles_.insert(selected->path);
  }
  ++selected->usedSlots;
  SpillLocation location{
      selected->path,
      slotIndex * selected->slotBytes,
      logicalBytes,
      storedBytes,
      selected->slotBytes,
      true,
      codec,
      disk_};
  liveLocations_.insert(LocationKey(location));
  return location;
}

SpillLocation SpillStore::AllocateDedicated(
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

void SpillStore::RollbackSmallSlot(const SpillLocation& location) noexcept {
  if (!location.smallSlot || location.slotBytes == 0) {
    return;
  }
  std::lock_guard<std::mutex> l(mutex_);
  liveLocations_.erase(LocationKey(location));
  const auto classIndex = SmallClassFor(location.slotBytes);
  if (!classIndex.has_value()) {
    return;
  }
  const auto slotIndex = location.offset / location.slotBytes;
  auto& cls = smallClasses_[*classIndex];
  for (auto& slab : cls.slabs) {
    if (!slab.deleted && slab.path == location.path) {
      if (slab.usedSlots > 0) {
        --slab.usedSlots;
      }
      slab.freeSlots.push_back(slotIndex);
      return;
    }
  }
}

void SpillStore::WriteToLocation(
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

void SpillStore::RegisterLiveFile(const std::string& path) {
  std::lock_guard<std::mutex> l(mutex_);
  liveFiles_.insert(path);
}

bool SpillStore::ForgetLiveFile(const std::string& path) noexcept {
  std::lock_guard<std::mutex> l(mutex_);
  liveLocations_.erase(LocationKey(path, 0));
  return liveFiles_.erase(path) != 0;
}

} // namespace bytedance::bolt::memory::bm
