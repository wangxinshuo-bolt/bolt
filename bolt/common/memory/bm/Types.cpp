/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/Types.h"

#include <atomic>

namespace bytedance::bolt::memory::bm {
namespace {

class NoOpCounter final : public Counter {
 public:
  void Add(uint64_t /*value*/) override {}
};

class NoOpGauge final : public Gauge {
 public:
  void Set(int64_t /*value*/) override {}
  void Add(int64_t /*delta*/) override {}
};

class NoOpHistogram final : public Histogram {
 public:
  void Observe(double /*value*/) override {}
};

class NoOpRegistry final : public MetricsRegistry {
 public:
  Counter& GetCounter(std::string_view /*name*/, std::string_view /*labels*/)
      override {
    return counter_;
  }
  Gauge& GetGauge(std::string_view /*name*/, std::string_view /*labels*/)
      override {
    return gauge_;
  }
  Histogram& GetHistogram(
      std::string_view /*name*/,
      std::string_view /*labels*/) override {
    return histogram_;
  }

 private:
  NoOpCounter counter_;
  NoOpGauge gauge_;
  NoOpHistogram histogram_;
};

} // namespace

MetricsRegistry& NoOpMetricsRegistry() {
  static NoOpRegistry instance;
  return instance;
}

const char* ToString(MemoryTag tag) {
  switch (tag) {
    case MemoryTag::kMetadata:
      return "metadata";
    case MemoryTag::kHashTable:
      return "hash_table";
    case MemoryTag::kSort:
      return "sort";
    case MemoryTag::kShuffle:
      return "shuffle";
    case MemoryTag::kScanCache:
      return "scan_cache";
    case MemoryTag::kOperatorState:
      return "operator_state";
    case MemoryTag::kExtension:
      return "extension";
    case MemoryTag::kInternal:
      return "internal";
    case MemoryTag::kNumTags:
      return "num_tags";
  }
  return "unknown";
}

const char* ToString(ReservationKind kind) {
  switch (kind) {
    case ReservationKind::kNormal:
      return "normal";
    case ReservationKind::kPinned:
      return "pinned";
    case ReservationKind::kScratch:
      return "scratch";
    case ReservationKind::kScratchEmergency:
      return "scratch_emergency";
  }
  return "unknown";
}

const char* ToString(EvictPolicy policy) {
  switch (policy) {
    case EvictPolicy::kSpillToDisk:
      return "spill_to_disk";
    case EvictPolicy::kDiscard:
      return "discard";
    case EvictPolicy::kRecompute:
      return "recompute";
    case EvictPolicy::kPinnedForever:
      return "pinned_forever";
  }
  return "unknown";
}

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

const char* ToString(DiskKind kind) {
  switch (kind) {
    case DiskKind::kUnknown:
      return "unknown";
    case DiskKind::kHdd:
      return "hdd";
    case DiskKind::kSsd:
      return "ssd";
    case DiskKind::kNvme:
      return "nvme";
    case DiskKind::kNetworkFs:
      return "network_fs";
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

ReservationKind BodyReservationKind(EvictPolicy policy) {
  return policy == EvictPolicy::kPinnedForever ? ReservationKind::kPinned
                                               : ReservationKind::kNormal;
}

bool IsSpillPolicy(EvictPolicy policy) {
  return policy == EvictPolicy::kSpillToDisk;
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
      // PinnedForever blocks never enter the queue; classify as the most
      // expensive bucket so any stale node that slips in is scanned last.
      return EvictionCostClass::kSpill;
  }
  return EvictionCostClass::kSpill;
}

} // namespace bytedance::bolt::memory::bm
