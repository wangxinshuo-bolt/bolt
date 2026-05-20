/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/EvictionTypes.h"

namespace bytedance::bolt::memory::bm {

const char* ToString(BlockState state) {
  switch (state) {
    case BlockState::kInvalid:
      return "invalid";
    case BlockState::kLoaded:
      return "loaded";
    case BlockState::kLoading:
      return "loading";
    case BlockState::kSpilling:
      return "spilling";
    case BlockState::kSpilled:
      return "spilled";
    case BlockState::kDiscarded:
      return "discarded";
    case BlockState::kEvictedRecomputable:
      return "evicted_recomputable";
  }
  return "unknown";
}

const char* ToString(EvictionCostClass cost) {
  switch (cost) {
    case EvictionCostClass::kFreeOrCheap:
      return "free_or_cheap";
    case EvictionCostClass::kSpill:
      return "spill";
  }
  return "unknown";
}

const char* ToString(EvictResultKind kind) {
  switch (kind) {
    case EvictResultKind::kFreed:
      return "freed";
    case EvictResultKind::kScheduled:
      return "scheduled";
    case EvictResultKind::kBackpressured:
      return "backpressured";
    case EvictResultKind::kSkipped:
      return "skipped";
    case EvictResultKind::kFailed:
      return "failed";
  }
  return "unknown";
}

EvictionCostClass EvictionCostFor(EvictPolicy policy, BlockState state) {
  (void)state;
  switch (policy) {
    case EvictPolicy::kDiscard:
    case EvictPolicy::kRecompute:
      return EvictionCostClass::kFreeOrCheap;
    case EvictPolicy::kSpillToDisk:
      return EvictionCostClass::kSpill;
    case EvictPolicy::kPinnedForever:
      return EvictionCostClass::kSpill;
  }
  return EvictionCostClass::kSpill;
}

} // namespace bytedance::bolt::memory::bm
