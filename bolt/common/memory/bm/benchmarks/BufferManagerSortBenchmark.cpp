/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

DEFINE_uint64(
    bm_sort_benchmark_size_gb,
    0,
    "Logical data size in GB.");
DEFINE_uint64(
    bm_sort_benchmark_size_mb,
    2048,
    "Logical data size in MB. When non-zero, overrides size_gb for smoke tests.");
DEFINE_uint64(
    bm_sort_benchmark_memory_budget_mb,
    256,
    "Total resident BufferManager memory budget. The benchmark splits this "
    "budget evenly across the four sort tasks.");
DEFINE_uint32(
    bm_sort_benchmark_spill_worker_threads,
    4,
    "ProcessSpillService worker thread count. Use 0 to benchmark the "
    "synchronous backpressure fallback path.");
DEFINE_uint32(
    bm_sort_benchmark_disk_io_initial_queue_depth,
    16,
    "Initial disk I/O queue depth used by the spill service.");
DEFINE_uint32(
    bm_sort_benchmark_disk_io_min_queue_depth,
    1,
    "Minimum adaptive disk I/O queue depth used by the spill service.");
DEFINE_uint32(
    bm_sort_benchmark_disk_io_max_queue_depth,
    64,
    "Maximum adaptive disk I/O queue depth used by the spill service.");
DEFINE_string(
    bm_sort_benchmark_disk_kind,
    "probe",
    "Disk kind policy: probe, nvme, ssd, hdd, network_fs, or unknown. "
    "Non-probe values force the spill service disk kind.");
DEFINE_string(
    bm_sort_benchmark_weighted_block_sizes_kb,
    "1:4,4:8,16:8,64:8,256:8,1024:6,4096:4,8192:3,16384:2,32768:2,65536:1,131072:1",
    "Comma-separated weighted block-size profile in size_kb:weight format. "
    "The benchmark expands this profile into deterministic BufferManager block "
    "sizes.");
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
constexpr uint32_t kSortTaskCount = 4;

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

uint64_t parseBenchmarkSize() {
  if (FLAGS_bm_sort_benchmark_size_mb != 0) {
    return FLAGS_bm_sort_benchmark_size_mb * kMiB;
  }
  BOLT_USER_CHECK_GT(
      FLAGS_bm_sort_benchmark_size_gb,
      0,
      "bm_sort_benchmark_size_gb must be positive");
  return FLAGS_bm_sort_benchmark_size_gb * kGiB;
}

std::vector<uint64_t> parseBlockSizeProfile() {
  BOLT_USER_CHECK(
      !FLAGS_bm_sort_benchmark_weighted_block_sizes_kb.empty(),
      "bm_sort_benchmark_weighted_block_sizes_kb must not be empty");
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

DiskKind parseDiskKind() {
  const auto& value = FLAGS_bm_sort_benchmark_disk_kind;
  if (value == "probe" || value == "unknown") {
    return DiskKind::kUnknown;
  }
  if (value == "nvme") {
    return DiskKind::kNvme;
  }
  if (value == "ssd") {
    return DiskKind::kSsd;
  }
  if (value == "hdd") {
    return DiskKind::kHdd;
  }
  if (value == "network_fs") {
    return DiskKind::kNetworkFs;
  }
  BOLT_USER_FAIL(
      "Unsupported bm_sort_benchmark_disk_kind '{}'. Expected probe, nvme, "
      "ssd, hdd, network_fs, or unknown",
      value);
  return DiskKind::kUnknown;
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
    result +=
        fmt::format("{}x{}", formatBytes(counts[i].first), counts[i].second);
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

DataFingerprint combineFingerprint(
    const DataFingerprint& left,
    const DataFingerprint& right) {
  return DataFingerprint{
      .rows = left.rows + right.rows,
      .valueXor = left.valueXor ^ right.valueXor,
      .valueSum = left.valueSum + right.valueSum,
      .hashXor = left.hashXor ^ right.hashXor,
      .hashSum = left.hashSum + right.hashSum};
}

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

  uint64_t CounterValue(std::string_view name) const {
    std::lock_guard<std::mutex> l(mutex_);
    uint64_t total = 0;
    for (const auto& [key, counter] : counters_) {
      if (matchesMetricName(key, name)) {
        total += counter->value();
      }
    }
    return total;
  }

  int64_t GaugeValue(std::string_view name) const {
    std::lock_guard<std::mutex> l(mutex_);
    int64_t total = 0;
    for (const auto& [key, gauge] : gauges_) {
      if (matchesMetricName(key, name)) {
        total += gauge->value();
      }
    }
    return total;
  }

  uint64_t HistogramCount(std::string_view name) const {
    std::lock_guard<std::mutex> l(mutex_);
    uint64_t total = 0;
    for (const auto& [key, histogram] : histograms_) {
      if (matchesMetricName(key, name)) {
        total += histogram->count();
      }
    }
    return total;
  }

 private:
  static std::string makeKey(std::string_view name, std::string_view labels) {
    return labels.empty() ? std::string(name)
                          : fmt::format("{}{{{}}}", name, labels);
  }

  static bool matchesMetricName(
      const std::string& key,
      std::string_view name) {
    return key == name ||
        (key.size() > name.size() && key.compare(0, name.size(), name) == 0 &&
         key[name.size()] == '{');
  }

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<BenchmarkCounter>> counters_;
  std::unordered_map<std::string, std::unique_ptr<BenchmarkGauge>> gauges_;
  std::unordered_map<std::string, std::unique_ptr<BenchmarkHistogram>>
      histograms_;
};

void printFeatureCoverage(
    const BenchmarkMetricsRegistry& metrics,
    std::ostream& out) {
  const auto counter = [&](std::string_view name) {
    return metrics.CounterValue(name);
  };
  const auto histogram = [&](std::string_view name) {
    return metrics.HistogramCount(name);
  };
  const auto line = [&](std::string_view name, bool covered, uint64_t value) {
    out << "  " << name << " = " << (covered ? "yes" : "no")
        << " value=" << value << "\n";
  };

  out << "\nBufferManager feature coverage\n";
  line("reclaim", counter("bm_reclaim_requests_total") > 0,
       counter("bm_reclaim_requests_total"));
  line("spill_submit", counter("bm_spill_submit_total") > 0,
       counter("bm_spill_submit_total"));
  line("sync_backpressure_fallback",
       counter("bm_spill_backpressured_total") > 0,
       counter("bm_spill_backpressured_total"));
  line("async_spill_scheduled", counter("bm_spill_scheduled_total") > 0,
       counter("bm_spill_scheduled_total"));
  line("async_spill_executed", counter("bm_spill_executed_total") > 0,
       counter("bm_spill_executed_total"));
  line("async_wait_for_progress",
       histogram("bm_wait_for_spill_progress_duration_us") > 0,
       histogram("bm_wait_for_spill_progress_duration_us"));
  line("small_spill_slot", counter("bm_spill_small_slot_total") > 0,
       counter("bm_spill_small_slot_total"));
  line("dedicated_spill_file", counter("bm_spill_dedicated_file_total") > 0,
       counter("bm_spill_dedicated_file_total"));
  line("compression_attempt", counter("bm_spill_compress_attempt_total") > 0,
       counter("bm_spill_compress_attempt_total"));
  line("compression_saved_bytes",
       counter("bm_spill_compress_saved_bytes") > 0,
       counter("bm_spill_compress_saved_bytes"));
  line("compression_raw_fallback",
       counter("bm_spill_compress_fallback_raw_total") > 0,
       counter("bm_spill_compress_fallback_raw_total"));
  line("spill_read", counter("bm_spill_bytes_read") > 0,
       counter("bm_spill_bytes_read"));
  line("spill_release", counter("bm_spill_release_total") > 0,
       counter("bm_spill_release_total"));
  out << "  configured_spill_workers = "
      << FLAGS_bm_sort_benchmark_spill_worker_threads << "\n";
}

struct Run {
  std::vector<std::shared_ptr<BlockHandle>> blocks;
  uint64_t rows{0};
  uint64_t bytes{0};
};

class DeterministicRowGenerator {
 public:
  explicit DeterministicRowGenerator(uint64_t rowOffset) : nextRow_(rowOffset) {}

  uint64_t next() {
    return splitmix64(nextRow_++);
  }

 private:
  uint64_t nextRow_;
};

class TaskQuota {
 public:
  explicit TaskQuota(uint64_t budgetBytes) : budgetBytes_(budgetBytes) {}

  void ensureCanAllocate(BufferManager& manager, ByteCount bytes) const {
    const auto usage = manager.GetMemoryUsage();
    if (usage + bytes > budgetBytes_) {
      manager.Reclaim(usage + bytes - budgetBytes_);
    }
  }

 private:
  uint64_t budgetBytes_;
};

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
      const TaskQuota& quota,
      const std::vector<uint64_t>& blockRows,
      uint64_t totalRows)
      : manager_(manager),
        quota_(quota),
        blockRows_(blockRows),
        totalRows_(totalRows) {}

  void append(uint64_t value) {
    if (!current_.has_value()) {
      openBlock();
    }
    current_->data[current_->position++] = value;
    ++writtenRows_;
    if (current_->position == current_->rows) {
      closeBlock();
    }
  }

  Run finish() {
    closeBlock();
    BOLT_USER_CHECK_EQ(writtenRows_, totalRows_);
    return std::move(run_);
  }

 private:
  struct WritableBlock {
    BufferHandle handle;
    uint64_t* data{nullptr};
    uint64_t rows{0};
    uint64_t position{0};
  };

  uint64_t nextBlockRows() const {
    const auto remaining = totalRows_ - writtenRows_;
    return std::min(blockRows_[blockIndex_ % blockRows_.size()], remaining);
  }

  void openBlock() {
    BOLT_USER_CHECK_LT(writtenRows_, totalRows_);
    const auto rows = nextBlockRows();
    const auto bytes = rows * kBytesPerRow;
    quota_.ensureCanAllocate(manager_, bytes);
    auto handle = manager_.Allocate(
        AllocateOptions{.tag = MemoryTag::kSort,
                        .size = bytes,
                        .policy = EvictPolicy::kSpillToDisk,
                        .priority = Priority::kNormal,
                        .recoveryFn = nullptr});
    current_.emplace(WritableBlock{
        .handle = std::move(handle),
        .data = nullptr,
        .rows = rows,
        .position = 0});
    current_->data = reinterpret_cast<uint64_t*>(current_->handle.MutableData());
  }

  void closeBlock() {
    if (!current_.has_value()) {
      return;
    }
    const auto bytes = current_->position * kBytesPerRow;
    BOLT_USER_CHECK_EQ(current_->position, current_->rows);
    run_.rows += current_->position;
    run_.bytes += bytes;
    run_.blocks.push_back(current_->handle.Block());
    current_.reset();
    ++blockIndex_;
  }

  BufferManager& manager_;
  const TaskQuota& quota_;
  const std::vector<uint64_t>& blockRows_;
  uint64_t totalRows_;
  uint64_t writtenRows_{0};
  size_t blockIndex_{0};
  std::optional<WritableBlock> current_;
  Run run_;
};

class SortOperator {
 public:
  SortOperator(
      uint32_t taskId,
      uint64_t totalRows,
      BufferManager& manager,
      const TaskQuota& quota,
      const std::vector<uint64_t>& blockRows,
      DataFingerprint& expectedFingerprint)
      : taskId_(taskId),
        totalRows_(totalRows),
        manager_(manager),
        quota_(quota),
        blockRows_(blockRows),
        expectedFingerprint_(expectedFingerprint) {}

  void addInput(uint64_t value) {
    BOLT_USER_CHECK(!inputClosed_, "Cannot add input after noMoreInput");
    if (!input_.has_value()) {
      openInputBlock();
    }
    input_->data[input_->position++] = value;
    expectedFingerprint_.add(value);
    ++inputRows_;
    if (input_->position == input_->rows) {
      closeInputBlock();
    }
  }

  void noMoreInput() {
    BOLT_USER_CHECK(!inputClosed_, "noMoreInput called twice");
    inputClosed_ = true;
    closeInputBlock();
    BOLT_USER_CHECK_EQ(inputRows_, totalRows_);
  }

  void mergeAll() {
    BOLT_USER_CHECK(inputClosed_, "Cannot merge before noMoreInput");
    uint32_t pass = 0;
    while (runs_.size() > 1) {
      runs_ = mergePass(std::move(runs_), ++pass);
    }
    if (!runs_.empty()) {
      outputRun_ = std::move(runs_.front());
      outputReader_ = std::make_unique<RunReader>(manager_, outputRun_);
    }
  }

  bool hasNext() const {
    return outputReader_ != nullptr && outputReader_->hasValue();
  }

  uint64_t next() {
    BOLT_USER_CHECK(hasNext(), "SortOperator output is exhausted");
    const auto value = outputReader_->value();
    outputReader_->advance();
    return value;
  }

  uint64_t finalBlockCount() const {
    return outputRun_.blocks.size();
  }

 private:
  struct WritableBlock {
    BufferHandle handle;
    uint64_t* data{nullptr};
    uint64_t rows{0};
    uint64_t position{0};
  };

  uint64_t nextInputBlockRows() const {
    const auto remaining = totalRows_ - inputRows_;
    return std::min(blockRows_[inputBlockIndex_ % blockRows_.size()], remaining);
  }

  void openInputBlock() {
    BOLT_USER_CHECK_LT(inputRows_, totalRows_);
    const auto rows = nextInputBlockRows();
    const auto bytes = rows * kBytesPerRow;
    quota_.ensureCanAllocate(manager_, bytes);
    auto handle = manager_.Allocate(
        AllocateOptions{.tag = MemoryTag::kSort,
                        .size = bytes,
                        .policy = EvictPolicy::kSpillToDisk,
                        .priority = Priority::kNormal,
                        .recoveryFn = nullptr});
    input_.emplace(WritableBlock{
        .handle = std::move(handle),
        .data = nullptr,
        .rows = rows,
        .position = 0});
    input_->data = reinterpret_cast<uint64_t*>(input_->handle.MutableData());
  }

  void closeInputBlock() {
    if (!input_.has_value()) {
      return;
    }
    BOLT_USER_CHECK_EQ(input_->position, input_->rows);
    std::sort(input_->data, input_->data + input_->position);
    Run run;
    run.rows = input_->position;
    run.bytes = input_->position * kBytesPerRow;
    run.blocks.push_back(input_->handle.Block());
    input_.reset();
    runs_.push_back(std::move(run));
    ++inputBlockIndex_;
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
          "\rtask {} merge pass {}: {} / {} input runs",
          taskId_,
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
    RunWriter writer(manager_, quota_, blockRows_, left.rows + right.rows);
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

  uint32_t taskId_;
  uint64_t totalRows_;
  BufferManager& manager_;
  const TaskQuota& quota_;
  const std::vector<uint64_t>& blockRows_;
  DataFingerprint& expectedFingerprint_;
  uint64_t inputRows_{0};
  size_t inputBlockIndex_{0};
  bool inputClosed_{false};
  std::optional<WritableBlock> input_;
  std::vector<Run> runs_;
  Run outputRun_;
  std::unique_ptr<RunReader> outputReader_;
};

DiskIoConfig makeDiskIoConfig() {
  DiskIoConfig ioConfig;
  ioConfig.backend = DiskIoBackend::kSync;
  ioConfig.initialQueueDepth =
      static_cast<int>(FLAGS_bm_sort_benchmark_disk_io_initial_queue_depth);
  ioConfig.minQueueDepth =
      static_cast<int>(FLAGS_bm_sort_benchmark_disk_io_min_queue_depth);
  ioConfig.maxQueueDepth =
      static_cast<int>(FLAGS_bm_sort_benchmark_disk_io_max_queue_depth);
  return ioConfig;
}

BufferManagerProcessServicesConfig makeProcessServicesConfig(
    BenchmarkMetricsRegistry& metrics) {
  BufferManagerProcessServicesConfig config;
  config.metrics = &metrics;
  config.spill.spillDir = FLAGS_bm_sort_benchmark_spill_dir;
  config.spill.forcedKind = parseDiskKind();
  config.spill.workerThreadCount =
      FLAGS_bm_sort_benchmark_spill_worker_threads;
  config.spill.cleanupOnDestroy = FLAGS_bm_sort_benchmark_cleanup;
  config.spill.diskIo = makeDiskIoConfig();
  return config;
}

BufferManagerConfig makeBufferManagerConfig(
    uint32_t taskId,
    uint64_t dataBytes,
    BenchmarkMetricsRegistry& metrics) {
  BufferManagerConfig config;
  config.poolName =
      fmt::format("bm_sort_task_{}_{}", taskId, formatBytes(dataBytes));
  config.metrics = &metrics;
  return config;
}

struct SortTaskResult {
  uint32_t taskId{0};
  uint64_t rowOffset{0};
  uint64_t rows{0};
  uint64_t dataBytes{0};
  uint64_t runs{0};
  uint64_t generateAndSortMs{0};
  uint64_t mergeMs{0};
  uint64_t verifyMs{0};
  uint64_t totalMs{0};
  uint64_t orderedChecksum{0};
  DataFingerprint expected;
  DataFingerprint actual;
  BufferPoolSnapshot snapshot;
};

class SortBenchmark {
 public:
  SortBenchmark(
      uint32_t taskId,
      uint64_t rowOffset,
      uint64_t rows,
      uint64_t dataBytes,
      uint64_t memoryBudgetBytes,
      std::vector<uint64_t> blockSizes,
      BenchmarkMetricsRegistry& metrics)
      : taskId_(taskId),
        rowOffset_(rowOffset),
        rows_(rows),
        dataBytes_(dataBytes),
        memoryBudgetBytes_(memoryBudgetBytes),
        blockSizes_(std::move(blockSizes)),
        metrics_(metrics),
        manager_(
            memoryManager_,
            makeBufferManagerConfig(taskId, dataBytes, metrics_)) {
    BOLT_USER_CHECK(!blockSizes_.empty(), "block size profile must not be empty");
    for (const auto bytes : blockSizes_) {
      blockRows_.push_back(std::max<uint64_t>(1, bytes / kBytesPerRow));
    }
  }

  SortTaskResult run() {
    const auto totalStart = Clock::now();
    std::cout << fmt::format(
        "\n=== BufferManager sort task {}: {} rows ({}) ===\n",
        taskId_,
        rows_,
        formatBytes(dataBytes_));
    std::cout << fmt::format(
        "task={} rowOffset={} blockSizeProfile={} blockSizeDistribution={} "
        "memoryBudgetBytes={}\n",
        taskId_,
        rowOffset_,
        FLAGS_bm_sort_benchmark_weighted_block_sizes_kb,
        formatSizeDistribution(blockSizes_),
        memoryBudgetBytes_);

    const auto genStart = Clock::now();
    DeterministicRowGenerator generator(rowOffset_);
    TaskQuota quota(memoryBudgetBytes_);
    SortOperator sort(
        taskId_,
        rows_,
        manager_,
        quota,
        blockRows_,
        expectedFingerprint_);
    for (uint64_t i = 0; i < rows_; ++i) {
      sort.addInput(generator.next());
      if ((i + 1) % (1 << 16) == 0 || i + 1 == rows_) {
        std::cout << fmt::format(
            "\rtask {} addInput: {} / {} rows",
            taskId_,
            i + 1,
            rows_)
                  << std::flush;
      }
    }
    std::cout << "\n";
    sort.noMoreInput();
    const auto generateAndSortMs = elapsedMs(genStart);

    const auto mergeStart = Clock::now();
    sort.mergeAll();
    const auto mergeMs = elapsedMs(mergeStart);

    uint64_t orderedChecksum = 0;
    DataFingerprint actualFingerprint;
    uint64_t outputRows = 0;
    if (FLAGS_bm_sort_benchmark_verify) {
      const auto verifyStart = Clock::now();
      bool hasLast = false;
      uint64_t last = 0;
      while (sort.hasNext()) {
        const auto value = sort.next();
        if (hasLast) {
          BOLT_USER_CHECK_LE(last, value, "Final run is not sorted");
        }
        hasLast = true;
        last = value;
        actualFingerprint.add(value);
        orderedChecksum ^= splitmix64(value + outputRows);
        ++outputRows;
      }
      BOLT_USER_CHECK_EQ(outputRows, rows_);
      BOLT_USER_CHECK_EQ(actualFingerprint.rows, expectedFingerprint_.rows);
      BOLT_USER_CHECK_EQ(
          actualFingerprint.valueXor, expectedFingerprint_.valueXor);
      BOLT_USER_CHECK_EQ(
          actualFingerprint.valueSum, expectedFingerprint_.valueSum);
      BOLT_USER_CHECK_EQ(
          actualFingerprint.hashXor, expectedFingerprint_.hashXor);
      BOLT_USER_CHECK_EQ(
          actualFingerprint.hashSum, expectedFingerprint_.hashSum);
      verifyMs_ = elapsedMs(verifyStart);
    }
    if (!FLAGS_bm_sort_benchmark_verify) {
      actualFingerprint = expectedFingerprint_;
      outputRows = rows_;
    }

    const auto snapshot = manager_.Snapshot();
    std::cout << fmt::format(
        "\nTaskSummary: task={} rowOffset={} dataBytes={} rows={} runs={} "
        "generateSortMs={} "
        "mergeMs={} verifyMs={} totalMs={} orderedChecksum={} "
        "expectedRows={} actualRows={} expectedValueXor={} actualValueXor={} "
        "expectedValueSum={} actualValueSum={} expectedHashXor={} "
        "actualHashXor={} expectedHashSum={} actualHashSum={} usedTotal={} "
        "loadedBytes={} spilledBytes={}\n",
        taskId_,
        rowOffset_,
        dataBytes_,
        rows_,
        sort.finalBlockCount(),
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
        snapshot.usedSpilledBytes);
    return SortTaskResult{
        .taskId = taskId_,
        .rowOffset = rowOffset_,
        .rows = rows_,
        .dataBytes = dataBytes_,
        .runs = sort.finalBlockCount(),
        .generateAndSortMs = generateAndSortMs,
        .mergeMs = mergeMs,
        .verifyMs = verifyMs_,
        .totalMs = elapsedMs(totalStart),
        .orderedChecksum = orderedChecksum,
        .expected = expectedFingerprint_,
        .actual = actualFingerprint,
        .snapshot = snapshot};
  }

 private:
  uint32_t taskId_;
  uint64_t rowOffset_;
  uint64_t rows_;
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

class ConcurrentSortBenchmark {
 public:
  ConcurrentSortBenchmark(
      uint64_t dataBytes,
      uint64_t memoryBudgetBytes,
      std::vector<uint64_t> blockSizes,
      BenchmarkMetricsRegistry& metrics)
      : dataBytes_(dataBytes),
        memoryBudgetBytes_(memoryBudgetBytes),
        blockSizes_(std::move(blockSizes)),
        metrics_(metrics) {}

  void run() {
    const auto totalStart = Clock::now();
    const auto totalRows = dataBytes_ / kBytesPerRow;
    const auto perTaskBudget =
        std::max<uint64_t>(1, memoryBudgetBytes_ / kSortTaskCount);

    std::cout << fmt::format(
        "\n=== BufferManager concurrent sort benchmark: {} ===\n",
        formatBytes(dataBytes_));
    std::cout << fmt::format(
        "sortTasks={} processSpillWorkers={} totalMemoryBudgetBytes={} "
        "perTaskMemoryBudgetBytes={}\n",
        kSortTaskCount,
        FLAGS_bm_sort_benchmark_spill_worker_threads,
        memoryBudgetBytes_,
        perTaskBudget);

    std::vector<SortTaskResult> results(kSortTaskCount);
    std::vector<std::exception_ptr> errors(kSortTaskCount);
    std::vector<std::thread> threads;
    threads.reserve(kSortTaskCount);

    uint64_t rowOffset = 0;
    for (uint32_t taskId = 0; taskId < kSortTaskCount; ++taskId) {
      const auto taskRows =
          totalRows / kSortTaskCount + (taskId < totalRows % kSortTaskCount);
      const auto taskOffset = rowOffset;
      rowOffset += taskRows;
      threads.emplace_back([&, taskId, taskOffset, taskRows]() {
        try {
          SortBenchmark task(
              taskId,
              taskOffset,
              taskRows,
              taskRows * kBytesPerRow,
              perTaskBudget,
              blockSizes_,
              metrics_);
          results[taskId] = task.run();
        } catch (...) {
          errors[taskId] = std::current_exception();
        }
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }
    for (uint32_t taskId = 0; taskId < kSortTaskCount; ++taskId) {
      if (errors[taskId] != nullptr) {
        std::rethrow_exception(errors[taskId]);
      }
    }

    printSummary(results, elapsedMs(totalStart));
  }

 private:
  void printSummary(
      const std::vector<SortTaskResult>& results,
      uint64_t totalMs) const {
    DataFingerprint expected;
    DataFingerprint actual;
    BufferPoolSnapshot snapshot;
    uint64_t rows = 0;
    uint64_t runs = 0;
    uint64_t orderedChecksum = 0;
    uint64_t generateAndSortMs = 0;
    uint64_t mergeMs = 0;
    uint64_t verifyMs = 0;
    for (const auto& result : results) {
      rows += result.rows;
      runs += result.runs;
      orderedChecksum ^= result.orderedChecksum;
      expected = combineFingerprint(expected, result.expected);
      actual = combineFingerprint(actual, result.actual);
      snapshot.usedTotalBytes += result.snapshot.usedTotalBytes;
      snapshot.usedPinnedBytes += result.snapshot.usedPinnedBytes;
      snapshot.usedLoadedBytes += result.snapshot.usedLoadedBytes;
      snapshot.usedSpilledBytes += result.snapshot.usedSpilledBytes;
      generateAndSortMs = std::max(generateAndSortMs, result.generateAndSortMs);
      mergeMs = std::max(mergeMs, result.mergeMs);
      verifyMs = std::max(verifyMs, result.verifyMs);
    }

    BOLT_USER_CHECK_EQ(rows, dataBytes_ / kBytesPerRow);
    BOLT_USER_CHECK_EQ(actual.rows, expected.rows);
    BOLT_USER_CHECK_EQ(actual.valueXor, expected.valueXor);
    BOLT_USER_CHECK_EQ(actual.valueSum, expected.valueSum);
    BOLT_USER_CHECK_EQ(actual.hashXor, expected.hashXor);
    BOLT_USER_CHECK_EQ(actual.hashSum, expected.hashSum);

    metrics_.print(std::cout);
    printFeatureCoverage(metrics_, std::cout);
    std::cout << fmt::format(
        "\nSummary: dataBytes={} rows={} sortTasks={} runs={} "
        "generateSortMs={} mergeMs={} verifyMs={} totalMs={} "
        "orderedChecksum={} expectedRows={} actualRows={} "
        "expectedValueXor={} actualValueXor={} expectedValueSum={} "
        "actualValueSum={} expectedHashXor={} actualHashXor={} "
        "expectedHashSum={} actualHashSum={} usedTotal={} loadedBytes={} "
        "spilledBytes={} usedDiskBytes={}\n",
        dataBytes_,
        rows,
        kSortTaskCount,
        runs,
        generateAndSortMs,
        mergeMs,
        verifyMs,
        totalMs,
        orderedChecksum,
        expected.rows,
        actual.rows,
        expected.valueXor,
        actual.valueXor,
        expected.valueSum,
        actual.valueSum,
        expected.hashXor,
        actual.hashXor,
        expected.hashSum,
        actual.hashSum,
        snapshot.usedTotalBytes,
        snapshot.usedLoadedBytes,
        snapshot.usedSpilledBytes,
        ProcessSpillService::Instance().UsedDiskBytes());
  }

  uint64_t dataBytes_;
  uint64_t memoryBudgetBytes_;
  std::vector<uint64_t> blockSizes_;
  BenchmarkMetricsRegistry& metrics_;
};

void prepareSpillDirectory() {
  std::filesystem::remove_all(FLAGS_bm_sort_benchmark_spill_dir);
  std::filesystem::create_directories(FLAGS_bm_sort_benchmark_spill_dir);
}

} // namespace
} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  bytedance::bolt::memory::bm::BenchmarkMetricsRegistry metrics;
  bytedance::bolt::memory::bm::prepareSpillDirectory();
  bytedance::bolt::memory::bm::BufferManager::InitializeProcessServices(
      bytedance::bolt::memory::bm::makeProcessServicesConfig(metrics));

  const auto size = bytedance::bolt::memory::bm::parseBenchmarkSize();
  const uint64_t memoryBudgetBytes =
      FLAGS_bm_sort_benchmark_memory_budget_mb * 1024ULL * 1024ULL;
  auto blockSizes = bytedance::bolt::memory::bm::parseBlockSizeProfile();

  {
    bytedance::bolt::memory::bm::ConcurrentSortBenchmark benchmark(
        size, memoryBudgetBytes, std::move(blockSizes), metrics);
    benchmark.run();
  }
  bytedance::bolt::memory::bm::BufferManager::ResetProcessServicesForTesting();
  return 0;
}
