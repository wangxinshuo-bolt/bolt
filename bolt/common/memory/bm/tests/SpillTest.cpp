/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstring>

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/tests/BufferManagerTestUtil.h"

namespace bytedance::bolt::memory::bm {

TEST(SpillTest, spillBlockCanBeReloaded) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.memoryLimitBytes = 1 << 20,
                          .pinnedLimitBytes = 1 << 20,
                          .emergencyScratchBytes = 64 << 10,
                          .poolName = "bm_block_spill",
                          .spillClient = test::makeSpillClientConfig(
                              "bm_block_spill")});

  auto handle = manager.Allocate(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 256,
                      .policy = EvictPolicy::kSpillToDisk});
  std::memset(handle.MutableData(), 11, handle.Size());
  auto block = handle.Block();
  handle = BufferHandle();

  ASSERT_EQ(manager.Reclaim(256), 256);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
  ASSERT_EQ(block->State(), BlockState::kSpilled);

  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 11);
  ASSERT_EQ(manager.GetMemoryUsage(), 256);
}

TEST(SpillTest, synchronousSpillWorksWhenWorkersDisabled) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{
          .memoryLimitBytes = 1 << 20,
          .pinnedLimitBytes = 1 << 20,
          .emergencyScratchBytes = 64 << 10,
          .poolName = "bm_sync_spill",
          .spillClient = test::makeSpillClientConfig("bm_sync_spill")});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 512,
                      .policy = EvictPolicy::kSpillToDisk},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 19, bytes); });

  ASSERT_EQ(manager.Reclaim(512), 512);
  ASSERT_EQ(block->State(), BlockState::kSpilled);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);

  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 19);
}

TEST(SpillTest, pinnedSpillBlockIsNotReclaimed) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{
          .memoryLimitBytes = 1 << 20,
          .pinnedLimitBytes = 1 << 20,
          .emergencyScratchBytes = 64 << 10,
          .poolName = "bm_pinned_spill",
          .spillClient = test::makeSpillClientConfig("bm_pinned_spill")});

  auto handle = manager.Allocate(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 128,
                      .policy = EvictPolicy::kSpillToDisk});
  auto block = handle.Block();

  ASSERT_EQ(manager.Reclaim(128), 0);
  ASSERT_EQ(manager.GetMemoryUsage(), 128);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
}

TEST(SpillTest, compressThenSpillRoundTrip) {
  // Verifies the kCompressThenSpill state path: kLoaded -> kCompressed
  // (synchronous compress) -> kSpilled (async/sync spill) -> kLoaded after
  // Pin.
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{
          .memoryLimitBytes = 1 << 20,
          .pinnedLimitBytes = 1 << 20,
          .emergencyScratchBytes = 64 << 10,
          .poolName = "bm_compress_then_spill",
          .spillClient = test::makeSpillClientConfig(
              "bm_compress_then_spill")});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kHashTable,
                      .size = 256,
                      .policy = EvictPolicy::kCompressThenSpill},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 91, bytes); });

  ASSERT_GT(manager.Reclaim(256), 0);
  // After reclaim with 0 worker threads, the synchronous spill path should
  // have moved the block all the way to kSpilled. With the MVP passthrough
  // compressor the compress step is net-neutral, so reclaim falls through to
  // SpillToDisk on the kCompressed payload.
  ASSERT_EQ(block->State(), BlockState::kSpilled);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);

  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 91);
  ASSERT_EQ(pinned.Data()[255], 91);
}

TEST(SpillTest, compressInPlaceLeavesCompressedState) {
  // Drives only the compression half of kCompressThenSpill by exposing the
  // BlockHandle directly. This verifies the kLoaded -> kCompressed transition
  // independently of the async spill scheduler.
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{
          .memoryLimitBytes = 1 << 20,
          .pinnedLimitBytes = 1 << 20,
          .emergencyScratchBytes = 64 << 10,
          .poolName = "bm_compress_in_place",
          .spillClient = test::makeSpillClientConfig(
              "bm_compress_in_place")});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kHashTable,
                      .size = 128,
                      .policy = EvictPolicy::kCompressThenSpill},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 23, bytes); });

  // The MVP compressor is a passthrough; net freed bytes is zero but the
  // block still transitions to kCompressed so a subsequent spill attempt can
  // operate on the compressed payload.
  ASSERT_EQ(block->CompressInPlace(), 0);
  ASSERT_EQ(block->State(), BlockState::kCompressed);

  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 23);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
}

// Per design doc §14.2: when emergency scratch is zero, both kSpillToDisk and
// kCompressThenSpill must be rejected at allocate time so callers do not get
// silently degraded spill behaviour.
TEST(SpillTest, zeroEmergencyScratchRejectsSpillPolicies) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{
          .memoryLimitBytes = 1 << 20,
          .pinnedLimitBytes = 1 << 20,
          .emergencyScratchBytes = 0,
          .poolName = "bm_zero_emergency_spill",
          .spillClient = test::makeSpillClientConfig(
              "bm_zero_emergency_spill")});

  ASSERT_THROW(
      manager.Allocate(AllocateOptions{.tag = MemoryTag::kHashTable,
                                       .size = 1024,
                                       .policy = EvictPolicy::kSpillToDisk}),
      ::bytedance::bolt::BoltUserError);
  ASSERT_THROW(
      manager.Allocate(
          AllocateOptions{.tag = MemoryTag::kHashTable,
                          .size = 1024,
                          .policy = EvictPolicy::kCompressThenSpill}),
      ::bytedance::bolt::BoltUserError);
}

} // namespace bytedance::bolt::memory::bm
