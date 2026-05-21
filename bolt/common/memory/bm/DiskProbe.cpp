/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/DiskProbe.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"

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

  int get() const {
    return fd_;
  }

 private:
  int fd_{-1};
};

struct FreeDeleter {
  void operator()(void* ptr) const {
    std::free(ptr);
  }
};

using AlignedBuffer = std::unique_ptr<void, FreeDeleter>;

AlignedBuffer allocateAlignedBuffer(size_t alignment, size_t bytes) {
  void* ptr = nullptr;
  if (::posix_memalign(&ptr, alignment, bytes) != 0) {
    return AlignedBuffer(nullptr);
  }
  return AlignedBuffer(ptr);
}

DiskKind classifyDisk(
    uint64_t writeIops,
    uint64_t readIops,
    const DiskProbeConfig& config) {
  const auto iops = std::min(writeIops, readIops);
  if (iops >= config.nvmeMinIops) {
    return DiskKind::kNvme;
  }
  if (iops >= config.ssdMinIops) {
    return DiskKind::kSsd;
  }
  return DiskKind::kHdd;
}

off_t nextProbeOffset(
    uint64_t op,
    size_t bytes,
    uint64_t fileBytes,
    uint64_t stride) {
  const uint64_t blocks = std::max<uint64_t>(1, fileBytes / bytes);
  return static_cast<off_t>(((op * stride) % blocks) * bytes);
}

uint64_t countDirectIopsFor(
    int fd,
    void* buffer,
    size_t bytes,
    uint64_t fileBytes,
    uint64_t offsetStride,
    uint64_t writeFsyncEveryOps,
    std::chrono::milliseconds duration,
    bool write) {
  using clock = std::chrono::steady_clock;
  const auto deadline = clock::now() + duration;
  uint64_t ops = 0;
  while (clock::now() < deadline) {
    const off_t offset = nextProbeOffset(ops, bytes, fileBytes, offsetStride);
    ssize_t rc = write ? ::pwrite(fd, buffer, bytes, offset)
                       : ::pread(fd, buffer, bytes, offset);
    if (rc != static_cast<ssize_t>(bytes)) {
      break;
    }
    ++ops;
    if (write && writeFsyncEveryOps != 0 &&
        ops % writeFsyncEveryOps == 0 && ::fdatasync(fd) != 0) {
      break;
    }
  }
  if (write) {
    (void)::fdatasync(fd);
  }
  const auto millis = std::max<int64_t>(1, duration.count());
  return ops * 1000 / static_cast<uint64_t>(millis);
}

DiskProbeResult inactiveResult(DiskKind kind) {
  return DiskProbeResult{kind, 0, 0, false, false};
}

DiskProbeResult logProbeResult(
    const DiskProbeConfig& config,
    const DiskProbeResult& result,
    const std::string& reason,
    uint64_t probeFileBytes,
    size_t blockBytes) {
  BOLT_MEM_LOG(INFO) << "BufferManager disk probe result"
                     << " directory=" << config.directory
                     << " reason=" << reason
                     << " kind=" << ToString(result.kind)
                     << " active_probe=" << result.activeProbeRan
                     << " direct_io=" << result.directIoUsed
                     << " probe_file_bytes=" << probeFileBytes
                     << " block_bytes=" << blockBytes
                     << " duration_ms=" << config.duration.count()
                     << " write_iops=" << result.writeIops
                     << " read_iops=" << result.readIops
                     << " forced_kind=" << ToString(config.forcedKind)
                     << " fallback_kind=" << ToString(config.fallbackKind);
  return result;
}

} // namespace

DiskProbeResult ProbeDisk(const DiskProbeConfig& config) {
  if (config.forcedKind != DiskKind::kUnknown) {
    return logProbeResult(
        config,
        inactiveResult(config.forcedKind),
        "forced_kind",
        0,
        0);
  }
  if (config.duration.count() <= 0) {
    return logProbeResult(
        config,
        inactiveResult(config.fallbackKind),
        "disabled",
        0,
        0);
  }
  BOLT_USER_CHECK_GT(config.blockBytes, 0, "Disk probe block bytes must be > 0");
  BOLT_USER_CHECK_GT(
      config.probeFileBytes, 0, "Disk probe file bytes must be > 0");
  BOLT_USER_CHECK_GE(
      config.probeFileBytes,
      config.blockBytes,
      "Disk probe file bytes must be >= block bytes");
  BOLT_USER_CHECK_GT(
      config.offsetStride, 0, "Disk probe offset stride must be > 0");

  std::filesystem::create_directories(config.directory);
  const auto path =
      (std::filesystem::path(config.directory) / "bm_disk_probe.tmp").string();
  ScopedFd fd(::open(
      path.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_DIRECT, 0644));
  if (fd.get() < 0) {
    return logProbeResult(
        config,
        inactiveResult(config.fallbackKind),
        std::string("open_direct_failed:") + std::strerror(errno),
        config.probeFileBytes,
        config.blockBytes);
  }
  const int fallocateError =
      ::posix_fallocate(fd.get(), 0, static_cast<off_t>(config.probeFileBytes));
  if (fallocateError != 0) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    return logProbeResult(
        config,
        inactiveResult(config.fallbackKind),
        std::string("posix_fallocate_failed:") + std::strerror(fallocateError),
        config.probeFileBytes,
        config.blockBytes);
  }

  auto buffer = allocateAlignedBuffer(config.blockBytes, config.blockBytes);
  if (buffer == nullptr) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    return logProbeResult(
        config,
        inactiveResult(config.fallbackKind),
        "aligned_buffer_failed",
        config.probeFileBytes,
        config.blockBytes);
  }
  auto* bytes = static_cast<uint8_t*>(buffer.get());
  for (size_t i = 0; i < config.blockBytes; ++i) {
    bytes[i] = static_cast<uint8_t>(i % 251);
  }

  const auto half = std::chrono::milliseconds(
      std::max<int64_t>(1, config.duration.count() / 2));
  const auto writeIops = countDirectIopsFor(
      fd.get(),
      buffer.get(),
      config.blockBytes,
      config.probeFileBytes,
      config.offsetStride,
      config.writeFsyncEveryOps,
      half,
      true);
  (void)::posix_fadvise(fd.get(), 0, 0, POSIX_FADV_DONTNEED);
  const auto readIops = countDirectIopsFor(
      fd.get(),
      buffer.get(),
      config.blockBytes,
      config.probeFileBytes,
      config.offsetStride,
      config.writeFsyncEveryOps,
      half,
      false);
  std::error_code ec;
  std::filesystem::remove(path, ec);
  if (writeIops == 0 || readIops == 0) {
    return logProbeResult(
        config,
        inactiveResult(config.fallbackKind),
        "zero_iops",
        config.probeFileBytes,
        config.blockBytes);
  }

  const auto kind = classifyDisk(writeIops, readIops, config);
  return logProbeResult(
      config,
      DiskProbeResult{kind, writeIops, readIops, true, true},
      "completed",
      config.probeFileBytes,
      config.blockBytes);
}

} // namespace bytedance::bolt::memory::bm
