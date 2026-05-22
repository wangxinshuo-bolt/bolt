/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>

namespace bytedance::bolt::memory::bm {

// Logical byte counter. Always non-negative; arithmetic uses saturating
// subtraction inside BufferPool so accidental underflow clamps to 0
// rather than wrapping to a very large value.
using ByteCount = uint64_t;
// Mutable byte view into BufferManager-owned memory. Lifetime is bound to
// the originating BufferHandle / AccountedMemory; never store across pin
// release or block eviction.
using DataPtr = uint8_t*;
// Read-only counterpart of DataPtr with the same lifetime contract.
using ConstDataPtr = const uint8_t*;

// Coarse "what is this allocation for" tag used to attribute usage in
// counters and metric labels. Tags are pure accounting hints – they never
// influence quota limits, eviction order, or scheduling. kNumTags is a
// sentinel: passing it to any public API throws BoltUserError.
enum class MemoryTag : uint8_t {
  kMetadata,
  kHashTable,
  kSort,
  kShuffle,
  kScanCache,
  kOperatorState,
  kExtension,
  kInternal,
  kNumTags,
};

// Reservation class observed by BufferPool for usage accounting.
//   kNormal - default body memory.
//   kPinned - memory that may never be evicted.
enum class ReservationKind : uint8_t {
  kNormal,
  kPinned,
};

// Per-block externalization strategy selected at Allocate() time:
//   kSpillToDisk        – evictable: body bytes are written through
//                         SpillCoordinator and reloaded on Pin().
//   kDiscard            – evictable: body bytes are dropped permanently;
//                         further Pin() returns an invalid BufferHandle.
//   kRecompute          – evictable: body bytes are dropped and reproduced
//                         on next Pin() via AllocateOptions::recoveryFn
//                         (recoveryFn must be non-null).
//   kPinnedForever      – never evictable; never enters the eviction queue;
//                         body bytes are accounted as kPinned.
enum class EvictPolicy : uint8_t {
  kSpillToDisk,
  kDiscard,
  kRecompute,
  kPinnedForever,
};

// Caller-supplied importance hint. Inside the eviction queue, higher
// priority FIFOs are scanned LAST within the same cost class so that
// kCritical blocks are sacrificed only when no cheaper option exists.
enum class Priority : uint8_t {
  kLow,
  kNormal,
  kHigh,
  kCritical,
};

// Returns a stable lower-case debug string for 'tag'. Returns "unknown" for
// out-of-range values (never throws). Strings are static – safe to store.
const char* ToString(MemoryTag tag);
// Returns a stable lower-case debug string for 'kind' (e.g. "normal",
// "pinned", "scratch", "scratch_emergency"). Never throws.
const char* ToString(ReservationKind kind);
// Returns a stable lower-case debug string for 'policy' (e.g. "spill_to_disk",
// "discard", "recompute", "pinned_forever").
const char* ToString(EvictPolicy policy);

// Maps a block eviction policy to the reservation kind used for its body
// bytes. Pure function. kPinnedForever => kPinned; everything else =>
// kNormal.
ReservationKind BodyReservationKind(EvictPolicy policy);

// Returns true iff 'policy' can externalize block bytes to spill storage.
// Pure function; matches the set of policies that require process services to
// be initialized through BufferManager before use.
bool IsSpillPolicy(EvictPolicy policy);

} // namespace bytedance::bolt::memory::bm
