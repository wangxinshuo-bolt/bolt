/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
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

enum class DiskIoBackend {
  kSync,
  kUring,
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
  DiskIoBackend backend{DiskIoBackend::kUring};
  uint32_t ringEntries{128};
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

class DiskIoEngine {
 public:
  virtual ~DiskIoEngine() = default;

  virtual DiskIoCompletion Execute(const DiskIoRequest& request) = 0;
};

class SyncDiskIoEngine final : public DiskIoEngine {
 public:
  DiskIoCompletion Execute(const DiskIoRequest& request) override;
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
using SynchronousDiskIoExecutor = SyncDiskIoEngine;
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

class ProcessDiskIoService {
 public:
  static void ConfigureDefault(DiskIoConfig config);
  static void ConfigureDefaultIfNeeded(DiskIoConfig config);
  static ProcessDiskIoService& Instance();
  static void ResetForTesting();

  DiskIoScheduler& Scheduler();

 private:
  explicit ProcessDiskIoService(DiskIoConfig config);

  DiskIoConfig config_;
  std::unique_ptr<DiskIoScheduler> scheduler_;
};

std::unique_ptr<DiskIoEngine> CreateDiskIoEngine(const DiskIoConfig& config);

} // namespace bytedance::bolt::memory::bm
