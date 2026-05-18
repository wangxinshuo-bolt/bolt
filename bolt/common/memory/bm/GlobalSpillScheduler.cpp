/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/GlobalSpillScheduler.h"

#include <limits>
#include <mutex>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BlockHandle.h"

namespace bytedance::bolt::memory::bm {

GlobalSpillScheduler::GlobalSpillScheduler(
    uint32_t workerThreadCount,
    MetricsRegistry& metrics)
    : metrics_(metrics), workerThreadCount_(workerThreadCount) {}

GlobalSpillScheduler::~GlobalSpillScheduler() {
  Stop();
}

void GlobalSpillScheduler::Start() {
  std::lock_guard<std::mutex> l(mutex_);
  if (started_ || workerThreadCount_ == 0) {
    started_ = true;
    return;
  }
  started_ = true;
  for (uint32_t i = 0; i < workerThreadCount_; ++i) {
    workers_.emplace_back([this] { WorkerLoop(); });
  }
  BOLT_MEM_LOG(INFO) << "GlobalSpillScheduler started with "
                     << workerThreadCount_ << " workers";
}

void GlobalSpillScheduler::Stop() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopping_) {
      return;
    }
    stopping_ = true;
  }
  cv_.notify_all();
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
  // Per design doc §11.4 Stop must advance the progress epoch so any Reserve
  // waiter unblocks and observes the shutdown state.
  NotifyProgress();
}

uint64_t GlobalSpillScheduler::RegisterClient(uint64_t weight) {
  const auto id = nextClientId_.fetch_add(1, std::memory_order_relaxed);
  std::lock_guard<std::mutex> l(mutex_);
  ClientFairState state;
  state.weight = weight == 0 ? 1 : weight;
  clients_.emplace(id, std::move(state));
  return id;
}

void GlobalSpillScheduler::UnregisterClient(uint64_t clientId) {
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = clients_.find(clientId);
    if (it == clients_.end()) {
      return;
    }
    // Mark as unregistered and drop pending nodes; workers re-validate so
    // tearing down a client mid-flight is safe.
    it->second.registered = false;
    it->second.ready.clear();
    clients_.erase(it);
  }
  // Wake waiters and workers so they can observe the cleared queue.
  cv_.notify_all();
  NotifyProgress();
}

EvictResult GlobalSpillScheduler::SubmitSpill(EvictionNode node) {
  if (node.block.expired()) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  const auto clientId = node.clientId;
  if (clientId == 0) {
    return EvictResult{EvictResultKind::kFailed, 0};
  }
  bool accepted = false;
  bool backpressured = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopping_) {
      return EvictResult{EvictResultKind::kFailed, 0};
    }
    auto it = clients_.find(clientId);
    if (it == clients_.end() || !it->second.registered) {
      return EvictResult{EvictResultKind::kFailed, 0};
    }
    it->second.ready.push_back(std::move(node));
    if (workerThreadCount_ == 0) {
      // No workers: the node is parked so Reserve can wait on a progress
      // event instead of busy-spinning, then report kBackpressured.
      backpressured = true;
    } else {
      accepted = true;
    }
  }
  cv_.notify_one();
  if (accepted) {
    return EvictResult{EvictResultKind::kScheduled, 0};
  }
  if (backpressured) {
    return EvictResult{EvictResultKind::kBackpressured, 0};
  }
  return EvictResult{EvictResultKind::kFailed, 0};
}

bool GlobalSpillScheduler::WaitForProgress(
    ByteCount /*bytesNeeded*/,
    std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> l(mutex_);
  const auto startEpoch = progressEpoch_;
  return progressCv_.wait_for(
      l, timeout, [&] { return stopping_ || progressEpoch_ != startEpoch; });
}

bool GlobalSpillScheduler::PickNextLocked(
    EvictionNode& out,
    uint64_t& ownerId) {
  // Fairness: pick the client with smallest virtualTime among those with a
  // non-empty ready queue. With few clients this linear scan is fine.
  uint64_t bestId = 0;
  double bestVtime = std::numeric_limits<double>::infinity();
  for (auto& [id, state] : clients_) {
    if (!state.registered || state.ready.empty()) {
      continue;
    }
    if (state.virtualTime < bestVtime) {
      bestVtime = state.virtualTime;
      bestId = id;
    }
  }
  if (bestId == 0) {
    return false;
  }
  auto it = clients_.find(bestId);
  out = std::move(it->second.ready.front());
  it->second.ready.pop_front();
  ownerId = bestId;
  return true;
}

void GlobalSpillScheduler::ChargeClientLocked(
    uint64_t clientId,
    ByteCount bytes) {
  auto it = clients_.find(clientId);
  if (it == clients_.end()) {
    return;
  }
  const auto weight =
      it->second.weight == 0 ? 1 : it->second.weight;
  it->second.virtualTime +=
      static_cast<double>(bytes) / static_cast<double>(weight);
}

void GlobalSpillScheduler::WorkerLoop() {
  for (;;) {
    EvictionNode node;
    uint64_t ownerId = 0;
    {
      std::unique_lock<std::mutex> l(mutex_);
      cv_.wait(l, [&] {
        if (stopping_) {
          return true;
        }
        for (auto& [id, state] : clients_) {
          if (state.registered && !state.ready.empty()) {
            return true;
          }
        }
        return false;
      });
      if (stopping_ && [&] {
            for (auto& [id, state] : clients_) {
              if (!state.ready.empty()) {
                return false;
              }
            }
            return true;
          }()) {
        return;
      }
      if (!PickNextLocked(node, ownerId)) {
        continue;
      }
      ++activeAttempts_;
    }

    // Lazy re-validation per design doc §11.3.
    auto base = node.block.lock();
    ByteCount bytes = 0;
    if (base != nullptr &&
        base->EvictionSequence() == node.evictionSequence) {
      try {
        auto handle = std::dynamic_pointer_cast<BlockHandle>(base);
        if (handle != nullptr && !handle->IsPinned()) {
          bytes = handle->SpillToDisk();
          BOLT_MEM_LOG(INFO)
              << "GlobalSpillScheduler processed block " << handle->Id()
              << " freed=" << bytes;
        }
      } catch (const std::exception& e) {
        BOLT_MEM_LOG(WARNING)
            << "GlobalSpillScheduler failed block: " << e.what();
      }
    }

    {
      std::lock_guard<std::mutex> l(mutex_);
      if (activeAttempts_ != 0) {
        --activeAttempts_;
      }
      ChargeClientLocked(ownerId, bytes);
    }
    NotifyProgress();
  }
}

void GlobalSpillScheduler::NotifyProgress() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++progressEpoch_;
  }
  progressCv_.notify_all();
}

} // namespace bytedance::bolt::memory::bm
