/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstring>
#include <filesystem>

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

TEST(SpillTest, compressThenSpillFallsBackWhenCompressionNeedsExtraQuota) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{
          .memoryLimitBytes = 1 << 20,
          .pinnedLimitBytes = 1 << 20,
          .emergencyScratchBytes = 512 << 10,
          .poolName = "bm_compress_fallback",
          .reserveWaitTimeout = std::chrono::milliseconds(10),
          .spillClient = test::makeSpillClientConfig(
              "bm_compress_fallback")});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kHashTable,
                      .size = 512 << 10,
                      .policy = EvictPolicy::kCompressThenSpill},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 37, bytes); });

  // The passthrough compressor would need a second 512KiB normal
  // reservation, but operator capacity is only memoryLimit-emergencyScratch =
  // 512KiB. Reclaim must not deadlock while the nested Reserve attempts reclaim;
  // it should restore kLoaded and fall through to direct spill.
  ASSERT_EQ(manager.Reclaim(512 << 10), 512 << 10);
  ASSERT_EQ(block->State(), BlockState::kSpilled);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);

  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 37);
  ASSERT_EQ(pinned.Data()[pinned.Size() - 1], 37);
}

TEST(SpillTest, spillLocationRoutesReleaseToOriginalStore) {
  // Use a directly-owned ProcessSpillService instead of the singleton so this
  // test can configure multiple stores without affecting other unit tests.
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_test_multi_spill_root";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root / "a");
  std::filesystem::create_directories(root / "b");

  ProcessSpillServiceConfig serviceConfig;
  serviceConfig.dirs.push_back(SpillDirConfig{.path = (root / "a").string()});
  serviceConfig.dirs.push_back(SpillDirConfig{.path = (root / "b").string()});
  serviceConfig.workerThreadCount = 0;
  serviceConfig.cleanupOnDestroy = true;
  serviceConfig.diskProbeDuration = std::chrono::milliseconds(0);
  auto service = ProcessSpillService::CreateForTesting(std::move(serviceConfig));

  SpillClientConfig clientConfig;
  clientConfig.enableSpill = true;
  clientConfig.tenantId = "bm_multi_store";
  auto client = service->CreateClient(std::move(clientConfig));

  uint8_t payload[4] = {1, 2, 3, 4};
  auto first = client->Write(MemoryTag::kShuffle, payload, sizeof(payload));
  auto second = client->Write(MemoryTag::kShuffle, payload, sizeof(payload));
  ASSERT_NE(first.storeIndex, second.storeIndex);
  ASSERT_TRUE(std::filesystem::exists(first.path));
  ASSERT_TRUE(std::filesystem::exists(second.path));

  client->Release(first);
  client->Release(second);
  ASSERT_FALSE(std::filesystem::exists(first.path));
  ASSERT_FALSE(std::filesystem::exists(second.path));
  ASSERT_EQ(service->UsedDiskBytes(), 0);
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
