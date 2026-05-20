/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/bm/MemoryTypes.h"

namespace bytedance::bolt::memory::bm {

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

ReservationKind BodyReservationKind(EvictPolicy policy) {
  return policy == EvictPolicy::kPinnedForever ? ReservationKind::kPinned
                                               : ReservationKind::kNormal;
}

bool IsSpillPolicy(EvictPolicy policy) {
  return policy == EvictPolicy::kSpillToDisk;
}

} // namespace bytedance::bolt::memory::bm
