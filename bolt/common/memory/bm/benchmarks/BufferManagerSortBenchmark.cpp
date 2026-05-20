/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

DEFINE_string(
    bm_sort_benchmark_sizes_gb,
    "10,100",
    "Comma-separated logical data sizes in GB. Default runs 10GB and 100GB.");
DEFINE_string(
    bm_sort_benchmark_sizes_mb,
    "",
    "Comma-separated logical data sizes in MB. Overrides sizes_gb for smoke "
    "tests.");
DEFINE_uint64(
    bm_sort_benchmark_memory_budget_mb,
    4096,
    "Resident BufferManager memory budget used to trigger Reclaim().");
DEFINE_uint64(
    bm_sort_benchmark_chunk_mb,
    256,
    "Fallback sorted run block size when block_sizes_kb is empty.");
DEFINE_string(
    bm_sort_benchmark_block_sizes_kb,
    "",
    "Comma-separated fixed BufferManager block sizes in KB. If set, these "
    "sizes are cycled and weighted_block_sizes_kb is ignored.");
DEFINE_string(
    bm_sort_benchmark_weighted_block_sizes_kb,
    "1:4,4:8,16:8,64:8,256:8,1024:6,4096:4,8192:3,16384:2,32768:2,65536:1,131072:1",
    "Comma-separated weighted block-size profile in size_kb:weight format. "
    "Used when block_sizes_kb is empty.");
DEFINE_uint64(
    bm_sort_benchmark_block_size_seed,
    0x9e3779b97f4a7c15ULL,
    "Seed used to deterministically expand weighted block-size choices.");
DEFINE_string(
    bm_sort_benchmark_spill_dir,
    "/tmp/bolt_bm_sort_benchmark_spill",
    "Directory used for BufferManager spill files.");
DEFINE_bool(
    bm_sort_benchmark_verify,
    true,
    "Verify the final sorted stream and include read-back cost in metrics.");
DEFINE_bool(
    bm_sort_benchmark_cleanup,
    true,
    "Remove BufferManager spill files when the process exits.");

namespace bytedance::bolt::memory::bm {
namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
constexpr uint64_t kMiB = 1024ULL * 1024ULL;
constexpr uint64_t kBytesPerRow = sizeof(uint64_t);

uint64_t elapsedMs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             Clock::now() - start)
      .count();
}

uint64_t splitmix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

std::vector<uint64_t> parseSizeUnits(const std::string& csv, uint64_t unit) {
  std::vector<uint64_t> sizes;
  size_t start = 0;
  while (start < csv.size()) {
    const auto comma = csv.find(',', start);
    const auto token = csv.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!token.empty()) {
      sizes.push_back(std::stoull(token) * unit);
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return sizes;
}

std::vector<uint64_t> parseBenchmarkSizes() {
  if (!FLAGS_bm_sort_benchmark_sizes_mb.empty()) {
    return parseSizeUnits(FLAGS_bm_sort_benchmark_sizes_mb, kMiB);
  }
  return parseSizeUnits(FLAGS_bm_sort_benchmark_sizes_gb, kGiB);
}

std::vector<uint64_t> parseBlockSizes(uint64_t fallbackBytes) {
  if (!FLAGS_bm_sort_benchmark_block_sizes_kb.empty()) {
    auto sizes = parseSizeUnits(FLAGS_bm_sort_benchmark_block_sizes_kb, 1024);
    BOLT_USER_CHECK(!sizes.empty(), "block_sizes_kb must not be empty");
    for (const auto bytes : sizes) {
      BOLT_USER_CHECK_GT(bytes, 0, "block_sizes_kb values must be positive");
    }
    return sizes;
  }
  if (FLAGS_bm_sort_benchmark_weighted_block_sizes_kb.empty()) {
    return {fallbackBytes};
  }
  std::vector<std::pair<uint64_t, uint64_t>> weighted;
  uint64_t totalWeight = 0;
  size_t start = 0;
  while (start < FLAGS_bm_sort_benchmark_weighted_block_sizes_kb.size()) {
    const auto comma =
        FLAGS_bm_sort_benchmark_weighted_block_sizes_kb.find(',', start);
    const auto token = FLAGS_bm_sort_benchmark_weighted_block_sizes_kb.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!token.empty()) {
      const auto colon = token.find(':');
      BOLT_USER_CHECK_NE(
          colon,
          std::string::npos,
          "weighted_block_sizes_kb entries must use size_kb:weight");
      const auto sizeKb = std::stoull(token.substr(0, colon));
      const auto weight = std::stoull(token.substr(colon + 1));
      BOLT_USER_CHECK_GT(sizeKb, 0, "weighted block size must be positive");
      BOLT_USER_CHECK_GT(weight, 0, "weighted block weight must be positive");
      weighted.emplace_back(sizeKb * 1024, weight);
      totalWeight += weight;
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  BOLT_USER_CHECK(!weighted.empty(), "weighted_block_sizes_kb must not be empty");
  BOLT_USER_CHECK_GT(totalWeight, 0, "total block-size weight must be positive");

  std::vector<uint64_t> sizes;
  sizes.reserve(1024);
  uint64_t state = FLAGS_bm_sort_benchmark_block_size_seed;
  for (size_t i = 0; i < 1024; ++i) {
    state = splitmix64(state + i);
    const auto pick = state % totalWeight;
    uint64_t cumulative = 0;
    for (const auto& [bytes, weight] : weighted) {
      cumulative += weight;
      if (pick < cumulative) {
        sizes.push_back(bytes);
        break;
      }
    }
  }
  return sizes;
}

std::string formatBytes(uint64_t bytes) {
  if (bytes % kGiB == 0) {
    return fmt::format("{}GiB", bytes / kGiB);
  }
  if (bytes % kMiB == 0) {
    return fmt::format("{}MiB", bytes / kMiB);
  }
  return fmt::format("{}B", bytes);
}

std::string formatSizeList(const std::vector<uint64_t>& sizes) {
  std::string result;
  for (size_t i = 0; i < sizes.size(); ++i) {
    if (i != 0) {
      result += ",";
    }
    result += formatBytes(sizes[i]);
  }
  return result;
}

std::string formatSizeDistribution(const std::vector<uint64_t>& sizes) {
  std::vector<std::pair<uint64_t, uint64_t>> counts;
  for (const auto size : sizes) {
    auto it = std::find_if(
        counts.begin(), counts.end(), [&](const auto& entry) {
          return entry.first == size;
        });
    if (it == counts.end()) {
      counts.emplace_back(size, 1);
    } else {
      ++it->second;
    }
  }
  std::sort(counts.begin(), counts.end());
  std::string result;
  for (size_t i = 0; i < counts.size(); ++i) {
    if (i != 0) {
      result += ",";
    }
    result += fmt::format("{}x{}", formatBytes(counts[i].first), counts[i].second);
  }
  return result;
}

struct DataFingerprint {
  uint64_t rows{0};
  uint64_t valueXor{0};
  uint64_t valueSum{0};
  uint64_t hashXor{0};
  uint64_t hashSum{0};

  void add(uint64_t value) {
    ++rows;
    valueXor ^= value;
    valueSum += value;
    const auto hash = splitmix64(value);
    hashXor ^= hash;
    hashSum += hash;
  }
};

class BenchmarkCounter final : public Counter {
 public:
  void Add(uint64_t value = 1) override {
    std::lock_guard<std::mutex> l(mutex_);
    value_ += value;
  }

  uint64_t value() const {
    std::lock_guard<std::mutex> l(mutex_);
    return value_;
  }

 private:
  mutable std::mutex mutex_;
  uint64_t value_{0};
};

class BenchmarkGauge final : public Gauge {
 public:
  void Set(int64_t value) override {
    std::lock_guard<std::mutex> l(mutex_);
    value_ = value;
  }

  void Add(int64_t delta) override {
    std::lock_guard<std::mutex> l(mutex_);
    value_ += delta;
  }

  int64_t value() const {
    std::lock_guard<std::mutex> l(mutex_);
    return value_;
  }

 private:
  mutable std::mutex mutex_;
  int64_t value_{0};
};

class BenchmarkHistogram final : public Histogram {
 public:
  void Observe(double value) override {
    std::lock_guard<std::mutex> l(mutex_);
    ++count_;
    sum_ += value;
    min_ = std::min(min_, value);
    max_ = std::max(max_, value);
  }

  uint64_t count() const {
    std::lock_guard<std::mutex> l(mutex_);
    return count_;
  }

  double sum() const {
    std::lock_guard<std::mutex> l(mutex_);
    return sum_;
  }

  double min() const {
    std::lock_guard<std::mutex> l(mutex_);
    return count_ == 0 ? 0 : min_;
  }

  double max() const {
    std::lock_guard<std::mutex> l(mutex_);
    return count_ == 0 ? 0 : max_;
  }

 private:
  mutable std::mutex mutex_;
  uint64_t count_{0};
  double sum_{0};
  double min_{std::numeric_limits<double>::max()};
  double max_{0};
};

class BenchmarkMetricsRegistry final : public MetricsRegistry {
 public:
  Counter& GetCounter(
      std::string_view name,
      std::string_view labels) override {
    std::lock_guard<std::mutex> l(mutex_);
    auto key = makeKey(name, labels);
    auto& counter = counters_[key];
    if (counter == nullptr) {
      counter = std::make_unique<BenchmarkCounter>();
    }
    return *counter;
  }

  Gauge& GetGauge(std::string_view name, std::string_view labels) override {
    std::lock_guard<std::mutex> l(mutex_);
    auto key = makeKey(name, labels);
    auto& gauge = gauges_[key];
    if (gauge == nullptr) {
      gauge = std::make_unique<BenchmarkGauge>();
    }
    return *gauge;
  }

  Histogram& GetHistogram(
      std::string_view name,
      std::string_view labels) override {
    std::lock_guard<std::mutex> l(mutex_);
    auto key = makeKey(name, labels);
    auto& histogram = histograms_[key];
    if (histogram == nullptr) {
      histogram = std::make_unique<BenchmarkHistogram>();
    }
    return *histogram;
  }

  void print(std::ostream& out) const {
    std::lock_guard<std::mutex> l(mutex_);
    out << "\nBufferManager metrics\n";
    out << "Counters\n";
    for (const auto& [key, counter] : counters_) {
      out << "  " << key << " = " << counter->value() << "\n";
    }
    out << "Gauges\n";
    for (const auto& [key, gauge] : gauges_) {
      out << "  " << key << " = " << gauge->value() << "\n";
    }
    out << "Histograms\n";
    for (const auto& [key, histogram] : histograms_) {
      const auto count = histogram->count();
      const auto avg = count == 0 ? 0 : histogram->sum() / count;
      out << "  " << key << " count=" << count << " avg=" << avg
          << " min=" << histogram->min() << " max=" << histogram->max()
          << " sum=" << histogram->sum() << "\n";
    }
  }

 private:
  static std::string makeKey(std::string_view name, std::string_view labels) {
    return labels.empty() ? std::string(name)
                          : fmt::format("{}{{{}}}", name, labels);
  }

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<BenchmarkCounter>> counters_;
  std::unordered_map<std::string, std::unique_ptr<BenchmarkGauge>> gauges_;
  std::unordered_map<std::string, std::unique_ptr<BenchmarkHistogram>>
      histograms_;
};

struct Run {
  std::vector<std::shared_ptr<BlockHandle>> blocks;
  uint64_t rows{0};
  uint64_t bytes{0};
};

class SortBenchmark {
 public:
  SortBenchmark(
      uint64_t dataBytes,
      uint64_t memoryBudgetBytes,
      std::vector<uint64_t> blockSizes,
      BenchmarkMetricsRegistry& metrics)
      : dataBytes_(dataBytes),
        memoryBudgetBytes_(memoryBudgetBytes),
        blockSizes_(std::move(blockSizes)),
        metrics_(metrics),
        manager_(
            memoryManager_,
            BufferManagerConfig{
                .poolName = fmt::format("bm_sort_{}", formatBytes(dataBytes_)),
                .metrics = &metrics_}) {
    BOLT_USER_CHECK(!blockSizes_.empty(), "block size profile must not be empty");
    for (const auto bytes : blockSizes_) {
      blockRows_.push_back(std::max<uint64_t>(1, bytes / kBytesPerRow));
    }
  }

  void run() {
    const auto totalStart = Clock::now();
    std::cout << fmt::format(
        "\n=== BufferManager sort benchmark: {} ===\n",
        formatBytes(dataBytes_));
    std::cout << fmt::format(
        "blockSizeProfile={} blockSizeDistribution={} memoryBudgetBytes={}\n",
        FLAGS_bm_sort_benchmark_block_sizes_kb.empty()
            ? FLAGS_bm_sort_benchmark_weighted_block_sizes_kb
            : FLAGS_bm_sort_benchmark_block_sizes_kb,
        formatSizeDistribution(blockSizes_),
        memoryBudgetBytes_);

    const auto genStart = Clock::now();
    auto runs = buildInitialRuns();
    const auto generateAndSortMs = elapsedMs(genStart);

    const auto mergeStart = Clock::now();
    uint32_t pass = 0;
    while (runs.size() > 1) {
      runs = mergePass(std::move(runs), ++pass);
    }
    const auto mergeMs = elapsedMs(mergeStart);

    uint64_t orderedChecksum = 0;
    DataFingerprint actualFingerprint;
    if (FLAGS_bm_sort_benchmark_verify && !runs.empty()) {
      const auto verifyStart = Clock::now();
      actualFingerprint = verifySorted(runs.front(), &orderedChecksum);
      verifyMs_ = elapsedMs(verifyStart);
    }

    const auto snapshot = manager_.Snapshot();
    metrics_.print(std::cout);
    std::cout << fmt::format(
        "\nSummary: dataBytes={} rows={} runs={} generateSortMs={} "
        "mergeMs={} verifyMs={} totalMs={} orderedChecksum={} "
        "expectedRows={} actualRows={} expectedValueXor={} actualValueXor={} "
        "expectedValueSum={} actualValueSum={} expectedHashXor={} "
        "actualHashXor={} expectedHashSum={} actualHashSum={} usedTotal={} "
        "loadedBytes={} spilledBytes={} usedDiskBytes={}\n",
        dataBytes_,
        dataBytes_ / kBytesPerRow,
        runs.empty() ? 0 : runs.front().blocks.size(),
        generateAndSortMs,
        mergeMs,
        verifyMs_,
        elapsedMs(totalStart),
        orderedChecksum,
        expectedFingerprint_.rows,
        actualFingerprint.rows,
        expectedFingerprint_.valueXor,
        actualFingerprint.valueXor,
        expectedFingerprint_.valueSum,
        actualFingerprint.valueSum,
        expectedFingerprint_.hashXor,
        actualFingerprint.hashXor,
        expectedFingerprint_.hashSum,
        actualFingerprint.hashSum,
        snapshot.usedTotalBytes,
        snapshot.usedLoadedBytes,
        snapshot.usedSpilledBytes,
        ProcessSpillService::Instance().UsedDiskBytes());
  }

 private:
  class RunReader {
   public:
    RunReader(BufferManager& manager, const Run& run)
        : manager_(manager), run_(run) {
      openNextBlock();
    }

    bool hasValue() const {
      return data_ != nullptr;
    }

    uint64_t value() const {
      return data_[position_];
    }

    void advance() {
      ++position_;
      if (position_ >= rowsInBlock_) {
        handle_ = BufferHandle();
        openNextBlock();
      }
    }

   private:
    void openNextBlock() {
      data_ = nullptr;
      position_ = 0;
      rowsInBlock_ = 0;
      if (blockIndex_ >= run_.blocks.size()) {
        return;
      }
      handle_ = manager_.Pin(run_.blocks[blockIndex_++]);
      BOLT_USER_CHECK(handle_.IsValid(), "Failed to pin sorted run block");
      rowsInBlock_ = handle_.Size() / kBytesPerRow;
      data_ = reinterpret_cast<const uint64_t*>(handle_.Data());
    }

    BufferManager& manager_;
    const Run& run_;
    size_t blockIndex_{0};
    BufferHandle handle_;
    const uint64_t* data_{nullptr};
    uint64_t position_{0};
    uint64_t rowsInBlock_{0};
  };

  class RunWriter {
   public:
    RunWriter(
        BufferManager& manager,
        const std::vector<uint64_t>& blockRows,
        uint64_t memoryBudgetBytes)
        : manager_(manager),
          blockRows_(blockRows),
          memoryBudgetBytes_(memoryBudgetBytes) {
      buffer_.reserve(maxBlockRows());
    }

    void append(uint64_t value) {
      buffer_.push_back(value);
      if (buffer_.size() == currentBlockRows()) {
        flush();
      }
    }

    Run finish() {
      flush();
      return std::move(run_);
    }

   private:
    void flush() {
      if (buffer_.empty()) {
        return;
      }
      const auto bytes = buffer_.size() * kBytesPerRow;
      auto block = manager_.AllocatePersistent(
          AllocateOptions{.tag = MemoryTag::kSort,
                          .size = bytes,
                          .policy = EvictPolicy::kSpillToDisk,
                          .priority = Priority::kNormal,
                          .recoveryFn = nullptr},
          [&](DataPtr data, ByteCount size) {
            BOLT_USER_CHECK_EQ(size, bytes);
            std::memcpy(data, buffer_.data(), bytes);
          });
      run_.rows += buffer_.size();
      run_.bytes += bytes;
      run_.blocks.push_back(std::move(block));
      buffer_.clear();
      ++blockIndex_;
      reclaimIfNeeded();
    }

    uint64_t currentBlockRows() const {
      return blockRows_[blockIndex_ % blockRows_.size()];
    }

    uint64_t maxBlockRows() const {
      return *std::max_element(blockRows_.begin(), blockRows_.end());
    }

    void reclaimIfNeeded() {
      const auto usage = manager_.GetMemoryUsage();
      if (usage > memoryBudgetBytes_) {
        manager_.Reclaim(usage - memoryBudgetBytes_);
      }
    }

    BufferManager& manager_;
    const std::vector<uint64_t>& blockRows_;
    uint64_t memoryBudgetBytes_;
    size_t blockIndex_{0};
    std::vector<uint64_t> buffer_;
    Run run_;
  };

  std::vector<Run> buildInitialRuns() {
    std::vector<uint64_t> values;
    values.reserve(*std::max_element(blockRows_.begin(), blockRows_.end()));
    const uint64_t totalRows = dataBytes_ / kBytesPerRow;
    std::vector<Run> runs;
    size_t blockIndex = 0;
    for (uint64_t base = 0; base < totalRows;) {
      const uint64_t rows =
          std::min(blockRows_[blockIndex % blockRows_.size()], totalRows - base);
      values.clear();
      for (uint64_t i = 0; i < rows; ++i) {
        const auto value = splitmix64(base + i);
        values.push_back(value);
        expectedFingerprint_.add(value);
      }
      std::sort(values.begin(), values.end());
      auto block = manager_.AllocatePersistent(
          AllocateOptions{.tag = MemoryTag::kSort,
                          .size = rows * kBytesPerRow,
                          .policy = EvictPolicy::kSpillToDisk,
                          .priority = Priority::kNormal,
                          .recoveryFn = nullptr},
          [&](DataPtr data, ByteCount size) {
            BOLT_USER_CHECK_EQ(size, rows * kBytesPerRow);
            std::memcpy(data, values.data(), size);
          });
      Run run;
      run.rows = rows;
      run.bytes = rows * kBytesPerRow;
      run.blocks.push_back(std::move(block));
      runs.push_back(std::move(run));
      reclaimIfNeeded();
      base += rows;
      ++blockIndex;
      std::cout << fmt::format(
          "\rinitial runs: {} / {} rows",
          std::min(base, totalRows),
          totalRows)
                << std::flush;
    }
    std::cout << "\n";
    return runs;
  }

  std::vector<Run> mergePass(std::vector<Run> runs, uint32_t pass) {
    std::vector<Run> next;
    next.reserve((runs.size() + 1) / 2);
    for (size_t i = 0; i < runs.size(); i += 2) {
      if (i + 1 == runs.size()) {
        next.push_back(std::move(runs[i]));
        continue;
      }
      next.push_back(mergeTwo(runs[i], runs[i + 1]));
      std::cout << fmt::format(
          "\rmerge pass {}: {} / {} input runs",
          pass,
          std::min(i + 2, runs.size()),
          runs.size())
                << std::flush;
    }
    std::cout << "\n";
    return next;
  }

  Run mergeTwo(const Run& left, const Run& right) {
    RunReader leftReader(manager_, left);
    RunReader rightReader(manager_, right);
    RunWriter writer(manager_, blockRows_, memoryBudgetBytes_);
    while (leftReader.hasValue() && rightReader.hasValue()) {
      if (leftReader.value() <= rightReader.value()) {
        writer.append(leftReader.value());
        leftReader.advance();
      } else {
        writer.append(rightReader.value());
        rightReader.advance();
      }
    }
    while (leftReader.hasValue()) {
      writer.append(leftReader.value());
      leftReader.advance();
    }
    while (rightReader.hasValue()) {
      writer.append(rightReader.value());
      rightReader.advance();
    }
    return writer.finish();
  }

  DataFingerprint verifySorted(const Run& run, uint64_t* orderedChecksum) {
    DataFingerprint actual;
    bool hasLast = false;
    uint64_t last = 0;
    uint64_t rows = 0;
    RunReader reader(manager_, run);
    while (reader.hasValue()) {
      const auto value = reader.value();
      if (hasLast) {
        BOLT_USER_CHECK_LE(last, value, "Final run is not sorted");
      }
      hasLast = true;
      last = value;
      actual.add(value);
      *orderedChecksum ^= splitmix64(value + rows);
      ++rows;
      reader.advance();
    }
    BOLT_USER_CHECK_EQ(rows, dataBytes_ / kBytesPerRow);
    BOLT_USER_CHECK_EQ(actual.rows, expectedFingerprint_.rows);
    BOLT_USER_CHECK_EQ(actual.valueXor, expectedFingerprint_.valueXor);
    BOLT_USER_CHECK_EQ(actual.valueSum, expectedFingerprint_.valueSum);
    BOLT_USER_CHECK_EQ(actual.hashXor, expectedFingerprint_.hashXor);
    BOLT_USER_CHECK_EQ(actual.hashSum, expectedFingerprint_.hashSum);
    return actual;
  }

  void reclaimIfNeeded() {
    const auto usage = manager_.GetMemoryUsage();
    if (usage > memoryBudgetBytes_) {
      manager_.Reclaim(usage - memoryBudgetBytes_);
    }
  }

  uint64_t dataBytes_;
  uint64_t memoryBudgetBytes_;
  std::vector<uint64_t> blockSizes_;
  std::vector<uint64_t> blockRows_;
  BenchmarkMetricsRegistry& metrics_;
  DataFingerprint expectedFingerprint_;
  uint64_t verifyMs_{0};
  MemoryManager memoryManager_;
  BufferManager manager_;
};

void configureServices(BenchmarkMetricsRegistry& metrics) {
  std::filesystem::remove_all(FLAGS_bm_sort_benchmark_spill_dir);
  std::filesystem::create_directories(FLAGS_bm_sort_benchmark_spill_dir);

  DiskIoConfig ioConfig;
  ioConfig.backend = DiskIoBackend::kSync;
  ioConfig.initialQueueDepth = 16;
  ioConfig.minQueueDepth = 1;
  ioConfig.maxQueueDepth = 64;
  ProcessDiskIoService::ConfigureDefault(ioConfig);

  ProcessSpillServiceConfig spillConfig;
  spillConfig.spillDir = FLAGS_bm_sort_benchmark_spill_dir;
  spillConfig.workerThreadCount = 0;
  spillConfig.cleanupOnDestroy = FLAGS_bm_sort_benchmark_cleanup;
  spillConfig.metrics = &metrics;
  ProcessSpillService::ConfigureDefault(std::move(spillConfig));
}

} // namespace
} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  bytedance::bolt::memory::bm::BenchmarkMetricsRegistry metrics;
  bytedance::bolt::memory::bm::configureServices(metrics);

  const auto sizes =
      bytedance::bolt::memory::bm::parseBenchmarkSizes();
  const uint64_t memoryBudgetBytes =
      FLAGS_bm_sort_benchmark_memory_budget_mb * 1024ULL * 1024ULL;
  const uint64_t chunkBytes =
      FLAGS_bm_sort_benchmark_chunk_mb * 1024ULL * 1024ULL;
  auto blockSizes = bytedance::bolt::memory::bm::parseBlockSizes(chunkBytes);

  for (const auto size : sizes) {
    bytedance::bolt::memory::bm::SortBenchmark benchmark(
        size, memoryBudgetBytes, blockSizes, metrics);
    benchmark.run();
  }
  bytedance::bolt::memory::bm::ProcessSpillService::ResetForTesting();
  return 0;
}
