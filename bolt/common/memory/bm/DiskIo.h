/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <array>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace bytedance::bolt::memory::bm {

enum class DiskIoOp {
  kRead,
  kWrite,
  kFsync,
};

enum class DiskIoPriority {
  kLow = 0,
  kMedium = 1,
  kHigh = 2,
};

struct AdaptiveQueueDepthConfig {
  size_t windowCompletionCount{8};
  size_t percentile{95};
  double latencyRisingRatio{1.25};
  double latencyStableRatio{1.10};
  double throughputDroppingRatio{0.95};
  double throughputHealthyRatio{0.98};
  double queueDepthDecreaseRatio{0.70};
  double queueDepthIncreaseRatio{1.20};
};

struct DiskIoConfig {
  uint32_t ringEntries{128};
  uint32_t workerThreadCount{1};
  int initialQueueDepth{16};
  int minQueueDepth{1};
  int maxQueueDepth{128};
  std::array<uint32_t, 3> priorityWeights{{1, 4, 8}};
  AdaptiveQueueDepthConfig adaptive;
};

struct DiskIoRequest {
  DiskIoOp op{DiskIoOp::kRead};
  DiskIoPriority priority{DiskIoPriority::kMedium};
  int fd{-1};
  void* buffer{nullptr};
  size_t size{0};
  uint64_t offset{0};
  uint64_t userData{0};
};

struct DiskIoCompletion {
  DiskIoOp op{DiskIoOp::kRead};
  DiskIoPriority priority{DiskIoPriority::kMedium};
  uint64_t userData{0};
  int64_t result{0};
  size_t requestedSize{0};
  uint64_t latencyUs{0};
};

struct DiskIoTask {
  DiskIoPriority priority{DiskIoPriority::kMedium};
  std::function<void()> run;
};

class DiskIoEngine {
 public:
  virtual ~DiskIoEngine() = default;

  virtual DiskIoCompletion Execute(const DiskIoRequest& request) = 0;
};

class UringDiskIoEngine final : public DiskIoEngine {
 public:
  explicit UringDiskIoEngine(unsigned entries);
  ~UringDiskIoEngine() override;

  UringDiskIoEngine(const UringDiskIoEngine&) = delete;
  UringDiskIoEngine& operator=(const UringDiskIoEngine&) = delete;

  DiskIoCompletion Execute(const DiskIoRequest& request) override;

 private:
  struct Impl;
  Impl* impl_{nullptr};
};

// Compatibility aliases for the earlier executor-shaped API used by a few
// tests while this layer is being integrated.
using DiskIoExecutor = DiskIoEngine;
using UringDiskIoExecutor = UringDiskIoEngine;

class AdaptiveQueueDepth {
 public:
  explicit AdaptiveQueueDepth(DiskIoConfig config);

  int Limit() const;
  void Observe(const DiskIoCompletion& completion);

 private:
  uint64_t Percentile95();
  double WindowThroughputMiBPerSec() const;

  mutable std::mutex mutex_;
  AdaptiveQueueDepthConfig config_;
  int depth_;
  int minDepth_;
  int maxDepth_;
  uint64_t windowBytes_{0};
  uint64_t windowLatencyUs_{0};
  uint64_t lastP95Us_{0};
  double lastThroughputMiBPerSec_{0.0};
  bool hasPreviousWindow_{false};
  std::vector<uint64_t> latencies_;
};

class DiskIoScheduler {
 public:
  DiskIoScheduler(
      std::unique_ptr<DiskIoEngine> engine,
      DiskIoConfig config = DiskIoConfig{});

  DiskIoCompletion SubmitAndWait(const DiskIoRequest& request);
  std::vector<DiskIoCompletion> SubmitAndWait(
      const std::vector<DiskIoRequest>& requests);

  int QueueDepthLimit() const;

 private:
  size_t PickNextLocked(const std::vector<DiskIoRequest>& requests);
  void ChargePriorityLocked(DiskIoPriority priority, size_t bytes);

  mutable std::mutex mutex_;
  std::unique_ptr<DiskIoEngine> engine_;
  DiskIoConfig config_;
  AdaptiveQueueDepth queueDepth_;
  std::array<int64_t, 3> deficit_{{0, 0, 0}};
};

class DiskIoTaskExecutor {
 public:
  static void ConfigureDefault(DiskIoConfig config);
  static void ConfigureDefaultIfNeeded(DiskIoConfig config);
  static DiskIoTaskExecutor& Instance();
  static void ResetForTesting();

  bool SubmitTask(DiskIoTask task);
  DiskIoScheduler& Scheduler();

 private:
  explicit DiskIoTaskExecutor(DiskIoConfig config);
  ~DiskIoTaskExecutor();

  friend struct std::default_delete<DiskIoTaskExecutor>;

  void StartWorkers();
  void StopWorkers();
  void WorkerLoop();
  size_t PickNextTaskLocked() const;

  DiskIoConfig config_;
  std::unique_ptr<DiskIoScheduler> scheduler_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<DiskIoTask> tasks_;
  bool stopping_{false};
  std::vector<std::thread> workers_;
};

std::unique_ptr<DiskIoEngine> CreateDiskIoEngine(const DiskIoConfig& config);

} // namespace bytedance::bolt::memory::bm
