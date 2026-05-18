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

// Reservation class observed by BufferPool when granting quota.
//   kNormal           – default body memory; counted against
//                       (memoryLimit - emergencyScratch).
//   kPinned           – memory that may never be evicted; additionally counted
//                       against pinnedLimit and rejected when it would push
//                       the pinned counter past pinnedLimit.
//   kScratch          – short-lived workspace bytes; counted into the scratch
//                       counter and the same shared (memoryLimit -
//                       emergencyScratch) pool as kNormal.
//   kScratchEmergency – may dip into the reserved emergency headroom; the
//                       slow-path reclaimer is intentionally NOT triggered
//                       for this kind so it can succeed during reclaim.
enum class ReservationKind : uint8_t {
  kNormal,
  kPinned,
  kScratch,
  kScratchEmergency,
};

// Per-block externalization strategy selected at Allocate() time:
//   kSpillToDisk        – evictable: body bytes are written through
//                         SpillClient and reloaded on Pin().
//   kDiscard            – evictable: body bytes are dropped permanently;
//                         further Pin() returns an invalid BufferHandle.
//   kRecompute          – evictable: body bytes are dropped and reproduced
//                         on next Pin() via AllocateOptions::recoveryFn
//                         (recoveryFn must be non-null).
//   kCompressThenSpill  – two-stage evictable: compress in place first,
//                         spill the compressed payload later.
//   kPinnedForever      – never evictable; never enters the eviction queue;
//                         body bytes are accounted as kPinned.
enum class EvictPolicy : uint8_t {
  kSpillToDisk,
  kDiscard,
  kRecompute,
  kCompressThenSpill,
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
// "discard", "recompute", "compress_then_spill", "pinned_forever").
const char* ToString(EvictPolicy policy);

// Maps a block eviction policy to the reservation kind used for its body
// bytes. Pure function. kPinnedForever => kPinned; everything else =>
// kNormal.
ReservationKind BodyReservationKind(EvictPolicy policy);

// Returns true iff 'policy' can externalize block bytes to spill storage,
// i.e. kSpillToDisk or kCompressThenSpill. Pure function; matches the set
// of policies that BufferManager rejects when SpillClientConfig::enableSpill
// is false.
bool IsSpillPolicy(EvictPolicy policy);

} // namespace bytedance::bolt::memory::bm
