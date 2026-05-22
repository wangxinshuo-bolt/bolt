/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "bolt/common/memory/bm/BufferManager.h"

namespace bytedance::bolt::memory::bm::test {

class RecordingCounter final : public Counter {
 public:
  void Add(uint64_t value = 1) override {
    std::lock_guard<std::mutex> l(mutex_);
    value_ += value;
  }

  uint64_t Value() const {
    std::lock_guard<std::mutex> l(mutex_);
    return value_;
  }

 private:
  mutable std::mutex mutex_;
  uint64_t value_{0};
};

class RecordingGauge final : public Gauge {
 public:
  void Set(int64_t value) override {
    std::lock_guard<std::mutex> l(mutex_);
    value_ = value;
  }

  void Add(int64_t delta) override {
    std::lock_guard<std::mutex> l(mutex_);
    value_ += delta;
  }

  int64_t Value() const {
    std::lock_guard<std::mutex> l(mutex_);
    return value_;
  }

 private:
  mutable std::mutex mutex_;
  int64_t value_{0};
};

class RecordingHistogram final : public Histogram {
 public:
  void Observe(double value) override {
    std::lock_guard<std::mutex> l(mutex_);
    ++count_;
    sum_ += value;
  }

  uint64_t Count() const {
    std::lock_guard<std::mutex> l(mutex_);
    return count_;
  }

  double Sum() const {
    std::lock_guard<std::mutex> l(mutex_);
    return sum_;
  }

 private:
  mutable std::mutex mutex_;
  uint64_t count_{0};
  double sum_{0};
};

class RecordingMetricsRegistry final : public MetricsRegistry {
 public:
  Counter& GetCounter(
      std::string_view name,
      std::string_view labels) override {
    std::lock_guard<std::mutex> l(mutex_);
    auto& counter = counters_[Key(name, labels)];
    if (counter == nullptr) {
      counter = std::make_unique<RecordingCounter>();
    }
    return *counter;
  }

  Gauge& GetGauge(std::string_view name, std::string_view labels) override {
    std::lock_guard<std::mutex> l(mutex_);
    auto& gauge = gauges_[Key(name, labels)];
    if (gauge == nullptr) {
      gauge = std::make_unique<RecordingGauge>();
    }
    return *gauge;
  }

  Histogram& GetHistogram(
      std::string_view name,
      std::string_view labels) override {
    std::lock_guard<std::mutex> l(mutex_);
    auto& histogram = histograms_[Key(name, labels)];
    if (histogram == nullptr) {
      histogram = std::make_unique<RecordingHistogram>();
    }
    return *histogram;
  }

  uint64_t CounterValue(
      std::string_view name,
      std::string_view labels = "") const {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = counters_.find(Key(name, labels));
    return it == counters_.end() ? 0 : it->second->Value();
  }

  int64_t GaugeValue(
      std::string_view name,
      std::string_view labels = "") const {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = gauges_.find(Key(name, labels));
    return it == gauges_.end() ? 0 : it->second->Value();
  }

  uint64_t HistogramCount(
      std::string_view name,
      std::string_view labels = "") const {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = histograms_.find(Key(name, labels));
    return it == histograms_.end() ? 0 : it->second->Count();
  }

 private:
  static std::string Key(std::string_view name, std::string_view labels) {
    return std::string(name) + "|" + std::string(labels);
  }

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<RecordingCounter>> counters_;
  std::unordered_map<std::string, std::unique_ptr<RecordingGauge>> gauges_;
  std::unordered_map<std::string, std::unique_ptr<RecordingHistogram>>
      histograms_;
};

// Returns a fresh per-test directory under the system temp dir. Old contents
// are wiped so successive runs do not accumulate stale spill files.
inline std::string testSpillDir(const std::string& name) {
  auto path = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path.string();
}

inline bool uringAvailableForTesting(std::string* reason = nullptr) {
  try {
    UringDiskIoEngine engine(8);
    return true;
  } catch (const std::exception& e) {
    if (reason != nullptr) {
      *reason = e.what();
    }
    return false;
  }
}

// Test fixture helper: lazily install BufferManager process services with
// a single root directory the suite can extend per BufferManager. Subsequent
// callers reuse the same configuration so the tests stay isolated from each
// other while respecting the "initialize once" contract. Returns the root
// directory the suite is rooted in.
inline std::string ensureTestSpillCoordinator() {
  static std::once_flag flag;
  static std::string root;
  std::call_once(flag, [&] {
    root = testSpillDir("bolt_bm_test_spill_root");
    BufferManagerProcessServicesConfig config;
    config.spill.spillDir = root;
    config.spill.workerThreadCount = 1;
    config.spill.cleanupOnDestroy = true;
    config.spill.diskProbeDuration = std::chrono::milliseconds(0);
    config.spill.diskIo.initialQueueDepth = 4;
    config.spill.diskIo.minQueueDepth = 1;
    config.spill.diskIo.maxQueueDepth = 16;
    BufferManager::InitializeProcessServices(std::move(config));
  });
  return root;
}

} // namespace bytedance::bolt::memory::bm::test
