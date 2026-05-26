/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

DEFINE_string(
    bm_spill_io_benchmark_spill_dir,
    "/tmp/bolt_bm_spill_io_benchmark",
    "Directory used for BufferManager spill files and disk baseline files.");
DEFINE_string(
    bm_spill_io_benchmark_threads,
    "4,8",
    "Comma-separated BufferManager owner/client thread counts.");
DEFINE_uint64(
    bm_spill_io_benchmark_small_block_bytes,
    4096,
    "Block size for the small-block IOPS case.");
DEFINE_uint64(
    bm_spill_io_benchmark_large_block_bytes,
    8ULL * 1024ULL * 1024ULL,
    "Block size for the large-block bandwidth case.");
DEFINE_uint64(
    bm_spill_io_benchmark_small_total_mb_per_thread,
    256,
    "Total logical bytes per owner thread for the small-block case, in MiB.");
DEFINE_uint64(
    bm_spill_io_benchmark_large_total_mb_per_thread,
    1024,
    "Total logical bytes per owner thread for the large-block case, in MiB.");
DEFINE_uint32(
    bm_spill_io_benchmark_disk_io_workers,
    4,
    "DiskIoTaskExecutor worker count used by BufferManager spill and prefetch.");
DEFINE_bool(
    bm_spill_io_benchmark_disable_compression,
    true,
    "Disable spill compression so the benchmark measures BufferManager I/O "
    "framework overhead without compression cost or compression savings.");
DEFINE_bool(
    bm_spill_io_benchmark_cleanup,
    true,
    "Remove benchmark spill and disk-baseline files before and after the run.");
DEFINE_bool(
    bm_spill_io_benchmark_run_disk_baseline,
    true,
    "Run disk write/read baseline.");
DEFINE_bool(
    bm_spill_io_benchmark_disk_baseline_direct_io,
    false,
    "Use O_DIRECT for the disk baseline. Disabled by default because the "
    "BufferManager spill path uses buffered I/O.");
DEFINE_bool(
    bm_spill_io_benchmark_disk_baseline_fsync_after_write,
    false,
    "Call fdatasync after each disk baseline writer finishes. Disabled by "
    "default because the application spill path does not require this "
    "durability boundary.");
DEFINE_bool(
    bm_spill_io_benchmark_disk_baseline_drop_cache_before_read,
    false,
    "Call posix_fadvise(DONTNEED) before disk baseline reads. Disabled by "
    "default so the baseline read path keeps the same page-cache-friendly "
    "semantics as the BufferManager spill path.");
DEFINE_bool(
    bm_spill_io_benchmark_run_pin_read,
    true,
    "Run normal BufferManager Pin() reload read case.");
DEFINE_bool(
    bm_spill_io_benchmark_run_prefetch_read,
    true,
    "Run BufferManager Prefetch() read case.");

namespace bytedance::bolt::memory::bm {
namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kMiB = 1024ULL * 1024ULL;
constexpr size_t kDirectIoAlignment = 4096;

struct FreeDeleter {
  void operator()(void* ptr) const {
    std::free(ptr);
  }
};

using AlignedBuffer = std::unique_ptr<void, FreeDeleter>;

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

struct CaseConfig {
  std::string name;
  uint64_t blockBytes{0};
  uint64_t totalBytesPerThread{0};
};

struct Throughput {
  uint64_t blocks{0};
  uint64_t bytes{0};
  uint64_t elapsedUs{0};
  uint64_t checksum{0};

  double seconds() const {
    return static_cast<double>(std::max<uint64_t>(1, elapsedUs)) / 1'000'000.0;
  }

  double iops() const {
    return static_cast<double>(blocks) / seconds();
  }

  double mibPerSec() const {
    return static_cast<double>(bytes) / static_cast<double>(kMiB) / seconds();
  }
};

struct DiskBaselineResult {
  bool directIoUsed{false};
  bool fdatasyncUsed{false};
  bool dropCacheBeforeRead{false};
  std::string error;
  Throughput write;
  Throughput read;
};

struct ThreadStartGate {
  std::barrier<>& ready;
  std::barrier<>& start;
  bool readyArrived{false};
  bool startArrived{false};

  void wait() {
    ready.arrive_and_wait();
    readyArrived = true;
    start.arrive_and_wait();
    startArrived = true;
  }

  void dropUnreached() {
    if (!readyArrived) {
      ready.arrive_and_drop();
    }
    if (!startArrived) {
      start.arrive_and_drop();
    }
  }
};

uint64_t elapsedUs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             Clock::now() - start)
      .count();
}

template <typename Worker>
uint64_t runTimedThreads(
    uint32_t threadCount,
    std::vector<uint64_t>& checksums,
    Worker worker) {
  std::barrier ready(static_cast<std::ptrdiff_t>(threadCount + 1));
  std::barrier start(static_cast<std::ptrdiff_t>(threadCount + 1));
  std::vector<std::thread> threads;
  std::vector<std::exception_ptr> errors(threadCount);
  threads.reserve(threadCount);

  for (uint32_t threadId = 0; threadId < threadCount; ++threadId) {
    threads.emplace_back([&, threadId] {
      ThreadStartGate gate{ready, start};
      try {
        checksums[threadId] = worker(threadId, gate);
      } catch (...) {
        errors[threadId] = std::current_exception();
        gate.dropUnreached();
      }
    });
  }

  ready.arrive_and_wait();
  const auto begin = Clock::now();
  start.arrive_and_wait();
  for (auto& thread : threads) {
    thread.join();
  }
  for (auto& error : errors) {
    if (error != nullptr) {
      std::rethrow_exception(error);
    }
  }
  return elapsedUs(begin);
}

std::vector<uint32_t> parseThreadCounts() {
  std::vector<uint32_t> counts;
  size_t start = 0;
  while (start < FLAGS_bm_spill_io_benchmark_threads.size()) {
    const auto comma = FLAGS_bm_spill_io_benchmark_threads.find(',', start);
    const auto token = FLAGS_bm_spill_io_benchmark_threads.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!token.empty()) {
      const auto value = static_cast<uint32_t>(std::stoul(token));
      BOLT_USER_CHECK_GT(value, 0, "thread count must be greater than zero");
      counts.push_back(value);
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  BOLT_USER_CHECK(!counts.empty(), "at least one thread count is required");
  return counts;
}

void validateCase(const CaseConfig& cfg) {
  BOLT_USER_CHECK_GT(cfg.blockBytes, 0, "block size must be greater than zero");
  if (FLAGS_bm_spill_io_benchmark_disk_baseline_direct_io) {
    BOLT_USER_CHECK_EQ(
        cfg.blockBytes % kDirectIoAlignment,
        0,
        "block size must be aligned to {} for baseline O_DIRECT",
        kDirectIoAlignment);
  }
  BOLT_USER_CHECK_GE(
      cfg.totalBytesPerThread,
      cfg.blockBytes,
      "total bytes per thread must be >= block bytes");
  BOLT_USER_CHECK_EQ(
      cfg.totalBytesPerThread % cfg.blockBytes,
      0,
      "total bytes per thread must be a multiple of block bytes");
}

AlignedBuffer allocateAlignedBuffer(size_t bytes) {
  void* ptr = nullptr;
  const auto rc = ::posix_memalign(&ptr, kDirectIoAlignment, bytes);
  BOLT_USER_CHECK_EQ(rc, 0, "posix_memalign failed: {}", std::strerror(rc));
  return AlignedBuffer(ptr);
}

void fillPayload(DataPtr data, uint64_t bytes, uint64_t seed) {
  uint64_t value = seed * 0x9e3779b97f4a7c15ULL + 0xd1b54a32d192ed03ULL;
  for (uint64_t i = 0; i < bytes; ++i) {
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    data[i] = static_cast<uint8_t>((value * 0x2545f4914f6cdd1dULL) >> 56);
  }
}

uint64_t touchPayload(ConstDataPtr data, uint64_t bytes) {
  if (data == nullptr || bytes == 0) {
    return 0;
  }
  return static_cast<uint64_t>(data[0]) +
      (static_cast<uint64_t>(data[bytes / 2]) << 8) +
      (static_cast<uint64_t>(data[bytes - 1]) << 16);
}

BufferManagerProcessServicesConfig makeProcessServicesConfig() {
  BufferManagerProcessServicesConfig config;
  config.spill.spillDir = FLAGS_bm_spill_io_benchmark_spill_dir;
  config.spill.workerThreadCount =
      FLAGS_bm_spill_io_benchmark_disk_io_workers;
  config.spill.cleanupOnDestroy = FLAGS_bm_spill_io_benchmark_cleanup;
  config.spill.diskProbeDuration = std::chrono::milliseconds(0);
  config.spill.unknownFallbackKind = DiskKind::kSsd;
  config.spill.diskIo.workerThreadCount =
      FLAGS_bm_spill_io_benchmark_disk_io_workers;
  config.spill.compression.enabled =
      !FLAGS_bm_spill_io_benchmark_disable_compression;
  return config;
}

BufferManagerConfig makeManagerConfig(const std::string& name) {
  BufferManagerConfig config;
  config.poolName = name;
  config.reserveWaitTimeout = std::chrono::seconds(60);
  config.spillEnabled = true;
  return config;
}

std::vector<std::shared_ptr<BlockHandle>> allocateBlocks(
    BufferManager& manager,
    const CaseConfig& cfg,
    uint32_t threadId,
    const std::string& phase) {
  const auto blockCount = cfg.totalBytesPerThread / cfg.blockBytes;
  std::vector<std::shared_ptr<BlockHandle>> blocks;
  blocks.reserve(blockCount);
  for (uint64_t i = 0; i < blockCount; ++i) {
    AllocateOptions options;
    options.tag = MemoryTag::kHashTable;
    options.size = cfg.blockBytes;
    options.policy = EvictPolicy::kSpillToDisk;
    options.priority = Priority::kNormal;
    auto block = manager.AllocatePersistent(
        options,
        [seed = (static_cast<uint64_t>(threadId) << 48) ^ i ^
             static_cast<uint64_t>(phase.size())](
            DataPtr data, ByteCount bytes) { fillPayload(data, bytes, seed); });
    blocks.push_back(std::move(block));
  }
  return blocks;
}

void reclaimAll(BufferManager& manager, uint64_t expectedBytes) {
  uint64_t reclaimed = 0;
  for (uint32_t attempts = 0; attempts < 1024; ++attempts) {
    const auto snapshot = manager.Snapshot();
    if (snapshot.usedLoadedBytes == 0) {
      break;
    }
    reclaimed += manager.Reclaim(snapshot.usedLoadedBytes);
  }
  const auto snapshot = manager.Snapshot();
  BOLT_USER_CHECK_EQ(
      snapshot.usedLoadedBytes,
      0,
      "failed to spill all loaded bytes; loaded={} spilled={} reclaimed={} "
      "expected={}",
      snapshot.usedLoadedBytes,
      snapshot.usedSpilledBytes,
      reclaimed,
      expectedBytes);
  BOLT_USER_CHECK_GE(
      snapshot.usedSpilledBytes,
      expectedBytes,
      "unexpected spilled bytes after reclaim");
}

Throughput runBmWrite(const CaseConfig& cfg, uint32_t threadCount) {
  std::vector<uint64_t> checksums(threadCount, 0);
  const auto elapsed = runTimedThreads(
      threadCount,
      checksums,
      [&](uint32_t threadId, ThreadStartGate& gate) -> uint64_t {
        MemoryManager::Options options;
        options.checkUsageLeak = false;
        MemoryManager memoryManager(options);
        BufferManager manager(
            memoryManager,
            makeManagerConfig(
                fmt::format("bm_spill_io_write_{}_{}", cfg.name, threadId)));
        auto blocks = allocateBlocks(manager, cfg, threadId, "write");
        uint64_t checksum = 0;
        for (const auto& block : blocks) {
          checksum ^= block->Id();
        }
        gate.wait();
        reclaimAll(manager, cfg.totalBytesPerThread);
        return checksum;
      });
  return Throughput{
      .blocks = (cfg.totalBytesPerThread / cfg.blockBytes) * threadCount,
      .bytes = cfg.totalBytesPerThread * threadCount,
      .elapsedUs = elapsed,
      .checksum = std::accumulate(checksums.begin(), checksums.end(), 0ULL)};
}

Throughput runBmPinRead(const CaseConfig& cfg, uint32_t threadCount) {
  std::vector<uint64_t> checksums(threadCount, 0);
  const auto elapsed = runTimedThreads(
      threadCount,
      checksums,
      [&](uint32_t threadId, ThreadStartGate& gate) -> uint64_t {
        MemoryManager::Options options;
        options.checkUsageLeak = false;
        MemoryManager memoryManager(options);
        BufferManager manager(
            memoryManager,
            makeManagerConfig(fmt::format(
                "bm_spill_io_pin_read_{}_{}", cfg.name, threadId)));
        auto blocks = allocateBlocks(manager, cfg, threadId, "pin_read");
        reclaimAll(manager, cfg.totalBytesPerThread);
        gate.wait();
        uint64_t checksum = 0;
        for (const auto& block : blocks) {
          auto handle = manager.Pin(block);
          checksum ^= touchPayload(handle.Data(), handle.Size());
        }
        return checksum;
      });
  return Throughput{
      .blocks = (cfg.totalBytesPerThread / cfg.blockBytes) * threadCount,
      .bytes = cfg.totalBytesPerThread * threadCount,
      .elapsedUs = elapsed,
      .checksum = std::accumulate(checksums.begin(), checksums.end(), 0ULL)};
}

Throughput runBmPrefetchRead(const CaseConfig& cfg, uint32_t threadCount) {
  std::vector<uint64_t> checksums(threadCount, 0);
  const auto elapsed = runTimedThreads(
      threadCount,
      checksums,
      [&](uint32_t threadId, ThreadStartGate& gate) -> uint64_t {
        MemoryManager::Options options;
        options.checkUsageLeak = false;
        MemoryManager memoryManager(options);
        BufferManager manager(
            memoryManager,
            makeManagerConfig(fmt::format(
                "bm_spill_io_prefetch_read_{}_{}", cfg.name, threadId)));
        auto blocks = allocateBlocks(manager, cfg, threadId, "prefetch_read");
        reclaimAll(manager, cfg.totalBytesPerThread);
        gate.wait();
        const auto result = manager.Prefetch(blocks);
        BOLT_USER_CHECK_EQ(
            result.submittedCount,
            blocks.size(),
            "prefetch submitted {} of {} blocks; skipped={} backpressured={}",
            result.submittedCount,
            blocks.size(),
            result.skippedCount,
            result.backpressuredCount);
        for (;;) {
          manager.Prefetch({});
          const auto snapshot = manager.Snapshot();
          if (snapshot.usedLoadedBytes >= cfg.totalBytesPerThread) {
            break;
          }
          std::this_thread::yield();
        }
        return result.submittedCount;
      });
  return Throughput{
      .blocks = (cfg.totalBytesPerThread / cfg.blockBytes) * threadCount,
      .bytes = cfg.totalBytesPerThread * threadCount,
      .elapsedUs = elapsed,
      .checksum = std::accumulate(checksums.begin(), checksums.end(), 0ULL)};
}

bool directIoOpenWorks(const std::string& path, std::string& error) {
  ScopedFd fd(::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_DIRECT, 0644));
  if (fd.get() < 0) {
    error = std::strerror(errno);
    return false;
  }
  return true;
}

int baselineOpenFlags(int baseFlags) {
  if (FLAGS_bm_spill_io_benchmark_disk_baseline_direct_io) {
    return baseFlags | O_DIRECT;
  }
  return baseFlags;
}

void writeFull(int fd, const void* buffer, uint64_t bytes, uint64_t offset) {
  const auto rc = ::pwrite(
      fd,
      buffer,
      static_cast<size_t>(bytes),
      static_cast<off_t>(offset));
  BOLT_USER_CHECK_EQ(
      rc,
      static_cast<ssize_t>(bytes),
      "pwrite failed or short write: rc={} bytes={} errno={}",
      rc,
      bytes,
      errno);
}

void readFull(int fd, void* buffer, uint64_t bytes, uint64_t offset) {
  const auto rc = ::pread(
      fd,
      buffer,
      static_cast<size_t>(bytes),
      static_cast<off_t>(offset));
  BOLT_USER_CHECK_EQ(
      rc,
      static_cast<ssize_t>(bytes),
      "pread failed or short read: rc={} bytes={} errno={}",
      rc,
      bytes,
      errno);
}

Throughput runDiskWriteBaseline(
    const std::string& path,
    const CaseConfig& cfg,
    uint32_t threadCount) {
  std::vector<uint64_t> checksums(threadCount, 0);
  const auto elapsed = runTimedThreads(
      threadCount,
      checksums,
      [&](uint32_t threadId, ThreadStartGate& gate) -> uint64_t {
        auto buffer = allocateAlignedBuffer(cfg.blockBytes);
        fillPayload(
            static_cast<DataPtr>(buffer.get()),
            cfg.blockBytes,
            static_cast<uint64_t>(threadId));
        ScopedFd fd(::open(path.c_str(), baselineOpenFlags(O_RDWR)));
        BOLT_USER_CHECK_GE(
            fd.get(), 0, "failed to open disk baseline file for write");
        const uint64_t beginOffset = cfg.totalBytesPerThread * threadId;
        const uint64_t endOffset = beginOffset + cfg.totalBytesPerThread;
        gate.wait();
        uint64_t checksum = 0;
        for (uint64_t offset = beginOffset; offset < endOffset;
             offset += cfg.blockBytes) {
          writeFull(fd.get(), buffer.get(), cfg.blockBytes, offset);
          checksum ^= offset;
        }
        if (FLAGS_bm_spill_io_benchmark_disk_baseline_fsync_after_write) {
          BOLT_USER_CHECK_EQ(::fdatasync(fd.get()), 0, "fdatasync failed");
        }
        return checksum;
      });
  return Throughput{
      .blocks = (cfg.totalBytesPerThread / cfg.blockBytes) * threadCount,
      .bytes = cfg.totalBytesPerThread * threadCount,
      .elapsedUs = elapsed,
      .checksum = std::accumulate(checksums.begin(), checksums.end(), 0ULL)};
}

Throughput runDiskReadBaseline(
    const std::string& path,
    const CaseConfig& cfg,
    uint32_t threadCount) {
  std::vector<uint64_t> checksums(threadCount, 0);
  const auto elapsed = runTimedThreads(
      threadCount,
      checksums,
      [&](uint32_t threadId, ThreadStartGate& gate) -> uint64_t {
        auto buffer = allocateAlignedBuffer(cfg.blockBytes);
        ScopedFd fd(::open(path.c_str(), baselineOpenFlags(O_RDONLY)));
        BOLT_USER_CHECK_GE(
            fd.get(), 0, "failed to open disk baseline file for read");
        if (FLAGS_bm_spill_io_benchmark_disk_baseline_drop_cache_before_read) {
          (void)::posix_fadvise(fd.get(), 0, 0, POSIX_FADV_DONTNEED);
        }
        const uint64_t beginOffset = cfg.totalBytesPerThread * threadId;
        const uint64_t endOffset = beginOffset + cfg.totalBytesPerThread;
        gate.wait();
        uint64_t checksum = 0;
        for (uint64_t offset = beginOffset; offset < endOffset;
             offset += cfg.blockBytes) {
          readFull(fd.get(), buffer.get(), cfg.blockBytes, offset);
          checksum ^= touchPayload(
              static_cast<ConstDataPtr>(buffer.get()), cfg.blockBytes);
        }
        return checksum;
      });
  return Throughput{
      .blocks = (cfg.totalBytesPerThread / cfg.blockBytes) * threadCount,
      .bytes = cfg.totalBytesPerThread * threadCount,
      .elapsedUs = elapsed,
      .checksum = std::accumulate(checksums.begin(), checksums.end(), 0ULL)};
}

DiskBaselineResult runDiskBaseline(const CaseConfig& cfg, uint32_t threadCount) {
  DiskBaselineResult result;
  const auto path = (std::filesystem::path(FLAGS_bm_spill_io_benchmark_spill_dir) /
                     fmt::format(
                         "disk_baseline_{}_threads_{}.dat",
                         cfg.name,
                         threadCount))
                        .string();
  std::error_code ec;
  std::filesystem::remove(path, ec);
  result.directIoUsed = FLAGS_bm_spill_io_benchmark_disk_baseline_direct_io;
  result.fdatasyncUsed =
      FLAGS_bm_spill_io_benchmark_disk_baseline_fsync_after_write;
  result.dropCacheBeforeRead =
      FLAGS_bm_spill_io_benchmark_disk_baseline_drop_cache_before_read;
  if (result.directIoUsed) {
    std::string directOpenError;
    if (!directIoOpenWorks(path, directOpenError)) {
      result.error = directOpenError;
      return result;
    }
  }

  {
    ScopedFd fd(::open(
        path.c_str(),
        baselineOpenFlags(O_CREAT | O_TRUNC | O_RDWR),
        0644));
    BOLT_USER_CHECK_GE(fd.get(), 0, "failed to open disk baseline file");
    const auto totalBytes =
        static_cast<off_t>(cfg.totalBytesPerThread * threadCount);
    const int fallocateError = ::posix_fallocate(fd.get(), 0, totalBytes);
    BOLT_USER_CHECK_EQ(
        fallocateError,
        0,
        "posix_fallocate failed: {}",
        std::strerror(fallocateError));
  }

  result.write = runDiskWriteBaseline(path, cfg, threadCount);
  if (result.dropCacheBeforeRead) {
    ScopedFd fd(::open(path.c_str(), baselineOpenFlags(O_RDONLY)));
    if (fd.get() >= 0) {
      (void)::posix_fadvise(fd.get(), 0, 0, POSIX_FADV_DONTNEED);
    }
  }
  result.read = runDiskReadBaseline(path, cfg, threadCount);
  if (FLAGS_bm_spill_io_benchmark_cleanup) {
    std::filesystem::remove(path, ec);
  }
  return result;
}

double ratio(double numerator, double denominator) {
  return denominator <= 0.0 ? 0.0 : numerator / denominator;
}

void printProgress(
    const CaseConfig& cfg,
    uint32_t threadCount,
    const std::string& phase,
    const std::string& status,
    uint64_t elapsedUs = 0) {
  std::cerr << fmt::format(
                   "BMSpillIoBenchmarkProgress case={} threads={} "
                   "disk_io_workers={} phase={} status={} elapsed_ms={:.3f}\n",
                   cfg.name,
                   threadCount,
                   FLAGS_bm_spill_io_benchmark_disk_io_workers,
                   phase,
                   status,
                   elapsedUs / 1000.0)
            << std::flush;
}

template <typename Fn>
auto runMeasuredPhase(
    const CaseConfig& cfg,
    uint32_t threadCount,
    const std::string& phase,
    Fn&& fn) {
  printProgress(cfg, threadCount, phase, "start");
  const auto phaseStart = Clock::now();
  try {
    auto result = fn();
    printProgress(cfg, threadCount, phase, "done", elapsedUs(phaseStart));
    return result;
  } catch (...) {
    printProgress(cfg, threadCount, phase, "failed", elapsedUs(phaseStart));
    throw;
  }
}

void printSummary(
    const CaseConfig& cfg,
    uint32_t threadCount,
    const Throughput& bmWrite,
    const Throughput& bmPinRead,
    const Throughput& bmPrefetchRead,
    const DiskBaselineResult& disk) {
  const bool small = cfg.blockBytes <= 4096;
  const double bmWriteValue = small ? bmWrite.iops() : bmWrite.mibPerSec();
  const double bmPinReadValue =
      small ? bmPinRead.iops() : bmPinRead.mibPerSec();
  const double bmPrefetchValue =
      small ? bmPrefetchRead.iops() : bmPrefetchRead.mibPerSec();
  const double diskWriteValue =
      small ? disk.write.iops() : disk.write.mibPerSec();
  const double diskReadValue = small ? disk.read.iops() : disk.read.mibPerSec();

  std::cout << fmt::format(
                   "BMSpillIoBenchmarkResult case={} threads={} "
                   "disk_io_workers={} compression_enabled={} "
                   "block_bytes={} total_bytes={} blocks={} "
                   "bm_write_elapsed_ms={:.3f} bm_pin_read_elapsed_ms={:.3f} "
                   "bm_prefetch_elapsed_ms={:.3f} "
                   "bm_write_iops={:.3f} bm_pin_read_iops={:.3f} "
                   "bm_prefetch_iops={:.3f} "
                   "bm_write_mib_per_sec={:.3f} "
                   "bm_pin_read_mib_per_sec={:.3f} "
                   "bm_prefetch_mib_per_sec={:.3f} "
                   "disk_direct_io_used={} disk_fdatasync_used={} "
                   "disk_drop_cache_before_read={} disk_error=\"{}\" "
                   "disk_write_iops={:.3f} disk_read_iops={:.3f} "
                   "disk_write_mib_per_sec={:.3f} "
                   "disk_read_mib_per_sec={:.3f} "
                   "write_ratio={:.6f} pin_read_ratio={:.6f} "
                   "prefetch_read_ratio={:.6f} "
                   "checksums={}:{}:{}:{}:{}\n",
                   cfg.name,
                   threadCount,
                   FLAGS_bm_spill_io_benchmark_disk_io_workers,
                   !FLAGS_bm_spill_io_benchmark_disable_compression,
                   cfg.blockBytes,
                   cfg.totalBytesPerThread * threadCount,
                   (cfg.totalBytesPerThread / cfg.blockBytes) * threadCount,
                   bmWrite.elapsedUs / 1000.0,
                   bmPinRead.elapsedUs / 1000.0,
                   bmPrefetchRead.elapsedUs / 1000.0,
                   bmWrite.iops(),
                   bmPinRead.iops(),
                   bmPrefetchRead.iops(),
                   bmWrite.mibPerSec(),
                   bmPinRead.mibPerSec(),
                   bmPrefetchRead.mibPerSec(),
                   disk.directIoUsed,
                   disk.fdatasyncUsed,
                   disk.dropCacheBeforeRead,
                   disk.error,
                   disk.write.iops(),
                   disk.read.iops(),
                   disk.write.mibPerSec(),
                   disk.read.mibPerSec(),
                   ratio(bmWriteValue, diskWriteValue),
                   ratio(bmPinReadValue, diskReadValue),
                   ratio(bmPrefetchValue, diskReadValue),
                   bmWrite.checksum,
                   bmPinRead.checksum,
                   bmPrefetchRead.checksum,
                   disk.write.checksum,
                   disk.read.checksum);
}

void runScenario(const CaseConfig& cfg, uint32_t threadCount) {
  validateCase(cfg);
  printProgress(cfg, threadCount, "scenario", "start");
  DiskBaselineResult disk;
  if (FLAGS_bm_spill_io_benchmark_run_disk_baseline) {
    disk = runMeasuredPhase(cfg, threadCount, "disk_baseline", [&] {
      return runDiskBaseline(cfg, threadCount);
    });
  }
  const auto bmWrite = runMeasuredPhase(cfg, threadCount, "bm_write", [&] {
    return runBmWrite(cfg, threadCount);
  });
  Throughput bmPinRead;
  if (FLAGS_bm_spill_io_benchmark_run_pin_read) {
    bmPinRead = runMeasuredPhase(cfg, threadCount, "bm_pin_read", [&] {
      return runBmPinRead(cfg, threadCount);
    });
  }
  Throughput bmPrefetchRead;
  if (FLAGS_bm_spill_io_benchmark_run_prefetch_read) {
    bmPrefetchRead =
        runMeasuredPhase(cfg, threadCount, "bm_prefetch_read", [&] {
          return runBmPrefetchRead(cfg, threadCount);
        });
  }
  printSummary(cfg, threadCount, bmWrite, bmPinRead, bmPrefetchRead, disk);
  printProgress(cfg, threadCount, "scenario", "done");
}

} // namespace

int runSpillIoBenchmark(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  std::filesystem::create_directories(FLAGS_bm_spill_io_benchmark_spill_dir);
  if (FLAGS_bm_spill_io_benchmark_cleanup) {
    std::filesystem::remove_all(FLAGS_bm_spill_io_benchmark_spill_dir);
    std::filesystem::create_directories(FLAGS_bm_spill_io_benchmark_spill_dir);
  }

  BufferManager::InitializeProcessServices(makeProcessServicesConfig());
  const std::vector<CaseConfig> cases{
      CaseConfig{
          "4k",
          FLAGS_bm_spill_io_benchmark_small_block_bytes,
          FLAGS_bm_spill_io_benchmark_small_total_mb_per_thread * kMiB},
      CaseConfig{
          "8mb",
          FLAGS_bm_spill_io_benchmark_large_block_bytes,
          FLAGS_bm_spill_io_benchmark_large_total_mb_per_thread * kMiB}};

  for (const auto threadCount : parseThreadCounts()) {
    for (const auto& cfg : cases) {
      runScenario(cfg, threadCount);
    }
  }

  BufferManager::ResetProcessServicesForTesting();
  if (FLAGS_bm_spill_io_benchmark_cleanup) {
    std::error_code ec;
    std::filesystem::remove_all(FLAGS_bm_spill_io_benchmark_spill_dir, ec);
  }
  return 0;
}

} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  return bytedance::bolt::memory::bm::runSpillIoBenchmark(argc, argv);
}
