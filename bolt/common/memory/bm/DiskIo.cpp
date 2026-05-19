/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/DiskIo.h"

#include <errno.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <mutex>

#ifdef IO_URING_SUPPORTED
#include <liburing.h>
#endif

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm {
namespace {

uint64_t elapsedUs(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

size_t priorityIndex(DiskIoPriority priority) {
  return static_cast<size_t>(priority);
}

void validateRequest(const DiskIoRequest& request) {
  BOLT_USER_CHECK_GE(request.fd, 0, "Invalid disk I/O file descriptor");
  if (request.op == DiskIoOp::kRead || request.op == DiskIoOp::kWrite) {
    BOLT_USER_CHECK_NOT_NULL(request.buffer, "Disk I/O buffer is null");
  }
}

void validateConfig(const DiskIoConfig& config) {
  BOLT_USER_CHECK_GT(
      config.initialQueueDepth, 0, "Disk IO initial queue depth must be > 0");
  BOLT_USER_CHECK_GT(
      config.minQueueDepth, 0, "Disk IO min queue depth must be > 0");
  BOLT_USER_CHECK_GE(
      config.initialQueueDepth,
      config.minQueueDepth,
      "Disk IO initial queue depth must be >= min queue depth");
  BOLT_USER_CHECK_GE(
      config.maxQueueDepth,
      config.initialQueueDepth,
      "Disk IO max queue depth must be >= initial queue depth");
  for (auto weight : config.priorityWeights) {
    BOLT_USER_CHECK_GT(weight, 0, "Disk IO priority weight must be > 0");
  }
}

int64_t runSync(const DiskIoRequest& request) {
  validateRequest(request);
  if (request.op == DiskIoOp::kFsync) {
    return ::fsync(request.fd) == 0 ? 0 : -errno;
  }

  size_t done = 0;
  while (done < request.size) {
    const auto* base = static_cast<const uint8_t*>(request.buffer);
    auto* mutableBase = static_cast<uint8_t*>(request.buffer);
    ssize_t rc = 0;
    if (request.op == DiskIoOp::kWrite) {
      rc = ::pwrite(
          request.fd,
          base + done,
          request.size - done,
          static_cast<off_t>(request.offset + done));
    } else {
      rc = ::pread(
          request.fd,
          mutableBase + done,
          request.size - done,
          static_cast<off_t>(request.offset + done));
    }
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -errno;
    }
    if (rc == 0) {
      break;
    }
    done += static_cast<size_t>(rc);
  }
  return static_cast<int64_t>(done);
}

struct SingletonState {
  std::mutex mutex;
  bool configured{false};
  DiskIoConfig pendingConfig;
  std::unique_ptr<ProcessDiskIoService> instance;
};

SingletonState& globalState() {
  static SingletonState state;
  return state;
}

} // namespace

DiskIoCompletion SyncDiskIoEngine::Execute(const DiskIoRequest& request) {
  const auto start = std::chrono::steady_clock::now();
  return DiskIoCompletion{
      request.op,
      request.priority,
      request.userData,
      runSync(request),
      request.size,
      elapsedUs(start)};
}

struct UringDiskIoEngine::Impl {
#ifdef IO_URING_SUPPORTED
  io_uring ring{};
#endif
};

UringDiskIoEngine::UringDiskIoEngine(unsigned entries) : impl_(new Impl()) {
#ifdef IO_URING_SUPPORTED
  int ret = io_uring_queue_init(entries, &impl_->ring, 0);
  if (ret < 0) {
    delete impl_;
    impl_ = nullptr;
    BOLT_USER_FAIL("io_uring_queue_init failed: {}", std::strerror(-ret));
  }
#else
  (void)entries;
  BOLT_USER_FAIL("io_uring is not enabled in this build");
#endif
}

UringDiskIoEngine::~UringDiskIoEngine() {
#ifdef IO_URING_SUPPORTED
  if (impl_ != nullptr) {
    io_uring_queue_exit(&impl_->ring);
  }
#endif
  delete impl_;
}

DiskIoCompletion UringDiskIoEngine::Execute(const DiskIoRequest& request) {
  validateRequest(request);
  const auto start = std::chrono::steady_clock::now();
#ifndef IO_URING_SUPPORTED
  (void)request;
  return DiskIoCompletion{
      DiskIoOp::kRead, DiskIoPriority::kMedium, 0, -ENOSYS, 0, elapsedUs(start)};
#else
  if (request.op == DiskIoOp::kFsync) {
    io_uring_sqe* sqe = io_uring_get_sqe(&impl_->ring);
    if (sqe == nullptr) {
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          -EBUSY,
          request.size,
          elapsedUs(start)};
    }
    io_uring_prep_fsync(sqe, request.fd, 0);
    int ret = io_uring_submit(&impl_->ring);
    if (ret < 0) {
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          ret,
          request.size,
          elapsedUs(start)};
    }

    io_uring_cqe* cqe = nullptr;
    ret = io_uring_wait_cqe(&impl_->ring, &cqe);
    if (ret < 0) {
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          ret,
          request.size,
          elapsedUs(start)};
    }
    const int res = cqe->res;
    io_uring_cqe_seen(&impl_->ring, cqe);
    return DiskIoCompletion{
        request.op,
        request.priority,
        request.userData,
        res,
        request.size,
        elapsedUs(start)};
  }

  size_t done = 0;
  while (done < request.size) {
    io_uring_sqe* sqe = io_uring_get_sqe(&impl_->ring);
    if (sqe == nullptr) {
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          -EBUSY,
          request.size,
          elapsedUs(start)};
    }

    auto* buffer = static_cast<uint8_t*>(request.buffer) + done;
    const auto size = request.size - done;
    const auto offset = request.offset + done;
    if (request.op == DiskIoOp::kWrite) {
      io_uring_prep_write(sqe, request.fd, buffer, size, offset);
    } else {
      io_uring_prep_read(sqe, request.fd, buffer, size, offset);
    }

    int ret = io_uring_submit(&impl_->ring);
    if (ret < 0) {
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          ret,
          request.size,
          elapsedUs(start)};
    }

    io_uring_cqe* cqe = nullptr;
    ret = io_uring_wait_cqe(&impl_->ring, &cqe);
    if (ret < 0) {
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          ret,
          request.size,
          elapsedUs(start)};
    }
    const int res = cqe->res;
    io_uring_cqe_seen(&impl_->ring, cqe);
    if (res < 0) {
      return DiskIoCompletion{
          request.op,
          request.priority,
          request.userData,
          res,
          request.size,
          elapsedUs(start)};
    }
    if (res == 0) {
      break;
    }
    done += static_cast<size_t>(res);
  }

  return DiskIoCompletion{
      request.op,
      request.priority,
      request.userData,
      static_cast<int64_t>(done),
      request.size,
      elapsedUs(start)};
#endif
}

AdaptiveQueueDepth::AdaptiveQueueDepth(DiskIoConfig config)
    : depth_(config.initialQueueDepth),
      minDepth_(config.minQueueDepth),
      maxDepth_(config.maxQueueDepth),
      targetP95LatencyUs_(config.targetP95LatencyUs) {
  validateConfig(config);
}

int AdaptiveQueueDepth::Limit() const {
  std::lock_guard<std::mutex> l(mutex_);
  return depth_;
}

void AdaptiveQueueDepth::Observe(const DiskIoCompletion& completion) {
  if (completion.op == DiskIoOp::kFsync || completion.result <= 0) {
    return;
  }

  std::lock_guard<std::mutex> l(mutex_);
  windowBytes_ += static_cast<uint64_t>(completion.result);
  latencies_.push_back(completion.latencyUs);
  if (latencies_.size() < 8) {
    return;
  }

  const auto p95 = Percentile95();
  const double mib = static_cast<double>(windowBytes_) / 1024.0 / 1024.0;
  if (p95 > targetP95LatencyUs_ * 2) {
    depth_ = std::max(minDepth_, static_cast<int>(depth_ * 0.7));
  } else if (p95 < targetP95LatencyUs_ && mib >= lastMiB_ * 0.98) {
    depth_ = std::min(maxDepth_, static_cast<int>(depth_ * 1.2) + 1);
  }
  lastMiB_ = mib;
  windowBytes_ = 0;
  latencies_.clear();
}

uint64_t AdaptiveQueueDepth::Percentile95() {
  std::sort(latencies_.begin(), latencies_.end());
  const auto index =
      std::min(latencies_.size() - 1, latencies_.size() * 95 / 100);
  return latencies_[index];
}

DiskIoScheduler::DiskIoScheduler(
    std::unique_ptr<DiskIoEngine> engine,
    DiskIoConfig config)
    : engine_(std::move(engine)),
      config_(config),
      queueDepth_(config) {
  validateConfig(config_);
  BOLT_USER_CHECK_NOT_NULL(engine_, "Disk IO scheduler requires an engine");
}

DiskIoCompletion DiskIoScheduler::SubmitAndWait(
    const DiskIoRequest& request) {
  auto completions = SubmitAndWait(std::vector<DiskIoRequest>{request});
  return completions.front();
}

std::vector<DiskIoCompletion> DiskIoScheduler::SubmitAndWait(
    const std::vector<DiskIoRequest>& requests) {
  std::lock_guard<std::mutex> l(mutex_);
  std::vector<DiskIoRequest> pending = requests;
  std::vector<DiskIoCompletion> completions;
  completions.reserve(pending.size());
  while (!pending.empty()) {
    const auto next = PickNextLocked(pending);
    auto req = pending[next];
    pending.erase(pending.begin() + static_cast<std::ptrdiff_t>(next));

    auto completion = engine_->Execute(req);
    queueDepth_.Observe(completion);
    ChargePriorityLocked(req.priority, req.size);
    completions.push_back(completion);
  }
  return completions;
}

int DiskIoScheduler::QueueDepthLimit() const {
  return queueDepth_.Limit();
}

size_t DiskIoScheduler::PickNextLocked(
    const std::vector<DiskIoRequest>& requests) {
  while (true) {
    int64_t bestScore = std::numeric_limits<int64_t>::min();
    size_t best = requests.size();
    for (size_t i = 0; i < requests.size(); ++i) {
      const auto idx = priorityIndex(requests[i].priority);
      deficit_[idx] += config_.priorityWeights[idx];
      if (deficit_[idx] > bestScore) {
        bestScore = deficit_[idx];
        best = i;
      }
    }
    if (best != requests.size()) {
      return best;
    }
  }
}

void DiskIoScheduler::ChargePriorityLocked(
    DiskIoPriority priority,
    size_t bytes) {
  const auto idx = priorityIndex(priority);
  const auto charge = static_cast<int64_t>(std::max<size_t>(1, bytes));
  deficit_[idx] -= charge;
}

std::unique_ptr<DiskIoEngine> CreateDiskIoEngine(
    const DiskIoConfig& config) {
  validateConfig(config);
  if (config.backend == DiskIoBackend::kSync) {
    return std::make_unique<SyncDiskIoEngine>();
  }
  return std::make_unique<UringDiskIoEngine>(config.ringEntries);
}

ProcessDiskIoService::ProcessDiskIoService(DiskIoConfig config)
    : config_(config),
      scheduler_(std::make_unique<DiskIoScheduler>(
          CreateDiskIoEngine(config_),
          config_)) {}

void ProcessDiskIoService::ConfigureDefault(DiskIoConfig config) {
  validateConfig(config);
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  BOLT_USER_CHECK(
      !state.configured && state.instance == nullptr,
      "ProcessDiskIoService::ConfigureDefault may be called only once");
  state.pendingConfig = config;
  state.configured = true;
}

void ProcessDiskIoService::ConfigureDefaultIfNeeded(DiskIoConfig config) {
  validateConfig(config);
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  if (state.configured || state.instance != nullptr) {
    return;
  }
  state.pendingConfig = config;
  state.configured = true;
}

ProcessDiskIoService& ProcessDiskIoService::Instance() {
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  if (state.instance != nullptr) {
    return *state.instance;
  }
  BOLT_USER_CHECK(
      state.configured,
      "ProcessDiskIoService::Instance() called without ConfigureDefault");
  state.instance.reset(new ProcessDiskIoService(state.pendingConfig));
  return *state.instance;
}

void ProcessDiskIoService::ResetForTesting() {
  auto& state = globalState();
  std::lock_guard<std::mutex> l(state.mutex);
  state.instance.reset();
  state.configured = false;
  state.pendingConfig = DiskIoConfig{};
}

DiskIoScheduler& ProcessDiskIoService::Scheduler() {
  return *scheduler_;
}

} // namespace bytedance::bolt::memory::bm
