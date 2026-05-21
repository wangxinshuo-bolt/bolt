/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/Evictor.h"

#include <algorithm>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BlockHandle.h"
#include "bolt/common/memory/bm/BufferManager.h"

namespace bytedance::bolt::memory::bm {
namespace {

constexpr size_t kCostClassCount = 2;
constexpr size_t kPriorityCount = 4;

size_t costIndex(EvictionCostClass cost) {
  return static_cast<size_t>(cost);
}

size_t priorityIndex(Priority priority) {
  return static_cast<size_t>(priority);
}

} // namespace

BlockEvictor::BlockEvictor(BufferManager& manager) : manager_(manager) {}

void BlockEvictor::SetSpillRequester(SpillRequester& requester) {
  spillRequester_ = requester;
}

void BlockEvictor::ClearSpillRequester() {
  spillRequester_.reset();
}

void BlockEvictor::Enqueue(EvictionNode node) {
  const auto cost = costIndex(node.cost);
  const auto priority = priorityIndex(node.priority);
  if (cost >= kCostClassCount || priority >= kPriorityCount) {
    return;
  }
  std::lock_guard<std::mutex> l(mutex_);
  queues_[cost][priority].push_back(std::move(node));
}

bool BlockEvictor::TryPopAnyCandidate(EvictionNode& out) {
  // Cheap-first scan order: kFreeOrCheap -> kSpill, and within
  // each cost class kLow -> kCritical.
  std::lock_guard<std::mutex> l(mutex_);
  for (size_t cost = 0; cost < kCostClassCount; ++cost) {
    for (size_t priority = 0; priority < kPriorityCount; ++priority) {
      auto& q = queues_[cost][priority];
      if (!q.empty()) {
        out = std::move(q.front());
        q.pop_front();
        return true;
      }
    }
  }
  return false;
}

std::shared_ptr<BlockHandle> BlockEvictor::ValidateNode(
    const EvictionNode& node) const {
  auto base = node.block.lock();
  if (base == nullptr) {
    return nullptr;
  }
  auto block = std::dynamic_pointer_cast<BlockHandle>(base);
  if (block == nullptr) {
    return nullptr;
  }
  if (block->EvictionSequence() != node.evictionSequence) {
    return nullptr;
  }
  if (block->IsPinned()) {
    return nullptr;
  }
  return block;
}

EvictResult BlockEvictor::TryEvictNodeSync(const EvictionNode& node) {
  auto block = ValidateNode(node);
  if (block == nullptr) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  // Sync path is reserved for cheap policies only; spill must go through
  // TryScheduleEvict.
  const auto policy = block->options_.policy;
  if (policy == EvictPolicy::kPinnedForever) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  if (policy == EvictPolicy::kSpillToDisk) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  const auto freed = block->TryEvict(0);
  if (freed == 0) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  return EvictResult{EvictResultKind::kFreed, freed};
}

EvictResult BlockEvictor::TryScheduleEvict(const EvictionNode& node) {
  auto block = ValidateNode(node);
  if (block == nullptr) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  if (!IsSpillPolicy(block->options_.policy)) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  if (!block->TryMarkSpillScheduled(node.evictionSequence)) {
    return EvictResult{EvictResultKind::kSkipped, 0};
  }
  if (!spillRequester_.has_value()) {
    block->ClearSpillScheduled();
    // No requester wired (e.g. shutting down). Reserve must observe a
    // failure rather than spin: the caller will fall through to its
    // backpressure-wait logic and then re-check.
    return EvictResult{EvictResultKind::kFailed, 0};
  }
  EvictionNode submitted = node;
  auto result = spillRequester_->get().SubmitSpill(std::move(submitted));
  if (result.kind != EvictResultKind::kScheduled) {
    block->ClearSpillScheduled();
  }
  return result;
}

bool BlockEvictor::WaitForProgress(
    ByteCount bytesNeeded,
    std::chrono::milliseconds timeout) {
  if (!spillRequester_.has_value()) {
    return false;
  }
  return spillRequester_->get().WaitForProgress(bytesNeeded, timeout);
}

} // namespace bytedance::bolt::memory::bm
