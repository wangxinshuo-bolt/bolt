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
namespace {

class CountingSpillRequester final : public SpillRequester {
 public:
  EvictResult SubmitSpill(EvictionNode /*node*/) override {
    ++submitCount;
    return EvictResult{EvictResultKind::kScheduled, 0};
  }

  bool WaitForProgress(
      ByteCount /*bytesNeeded*/,
      std::chrono::milliseconds /*timeout*/) override {
    return false;
  }

  int submitCount{0};
};

} // namespace

TEST(SpillTest, spillBlockCanBeReloaded) {
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_block_spill"});

  auto handle = manager.Allocate(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 256,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr});
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
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_sync_spill"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 512,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 19, bytes); });

  ASSERT_EQ(manager.Reclaim(512), 512);
  ASSERT_EQ(block->State(), BlockState::kSpilled);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);

  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 19);
}

TEST(SpillTest, pinnedSpillBlockIsNotReclaimed) {
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_pinned_spill"});

  auto handle = manager.Allocate(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 128,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr});
  auto block = handle.Block();

  ASSERT_EQ(manager.Reclaim(128), 0);
  ASSERT_EQ(manager.GetMemoryUsage(), 128);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
}

TEST(SpillTest, reclaimUsesEvictionQueueCostOrder) {
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_reclaim_queue_order"});

  auto spillBlock = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 128,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 1, bytes); });
  auto discardBlock = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 64,
                      .policy = EvictPolicy::kDiscard,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 2, bytes); });

  ASSERT_EQ(manager.Reclaim(64), 64);
  ASSERT_EQ(discardBlock->State(), BlockState::kDiscarded);
  ASSERT_EQ(spillBlock->State(), BlockState::kLoaded);
  ASSERT_EQ(manager.GetMemoryUsage(), 128);
}

TEST(SpillTest, spillCandidateIsNotSubmittedTwiceWhileScheduled) {
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_no_duplicate_submit"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 128,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 3, bytes); });
  CountingSpillRequester requester;
  dynamic_cast<BlockEvictor&>(manager.EvictionQueue())
      .SetSpillRequester(&requester);

  EvictionNode node;
  ASSERT_TRUE(manager.EvictionQueue().TryPopAnyCandidate(node));
  ASSERT_EQ(manager.EvictionQueue().TryScheduleEvict(node).kind,
            EvictResultKind::kScheduled);
  ASSERT_EQ(manager.EvictionQueue().TryScheduleEvict(node).kind,
            EvictResultKind::kSkipped);
  ASSERT_EQ(requester.submitCount, 1);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
}

TEST(SpillTest, spillLocationReleaseUsesSingleStore) {
  // Use a directly-owned ProcessSpillService instead of the singleton so this
  // test can inspect the store without affecting other unit tests.
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_test_single_spill_root";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);

  ProcessSpillServiceConfig serviceConfig;
  serviceConfig.spillDir = root.string();
  serviceConfig.workerThreadCount = 0;
  serviceConfig.cleanupOnDestroy = true;
  serviceConfig.diskProbeDuration = std::chrono::milliseconds(0);
  auto service = ProcessSpillService::CreateForTesting(std::move(serviceConfig));

  uint8_t payload[4] = {1, 2, 3, 4};
  auto location = service->Write(MemoryTag::kShuffle, payload, sizeof(payload));
  ASSERT_TRUE(std::filesystem::exists(location.path));
  ASSERT_GT(service->UsedDiskBytes(), 0);

  service->Release(location);
  ASSERT_FALSE(std::filesystem::exists(location.path));
  ASSERT_EQ(service->UsedDiskBytes(), 0);
}

TEST(SpillTest, spillPoliciesUseConfiguredProcessService) {
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_process_spill_service"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kHashTable,
                      .size = 1024,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 55, bytes); });
  ASSERT_EQ(manager.Reclaim(1024), 1024);
  ASSERT_EQ(block->State(), BlockState::kSpilled);
}

} // namespace bytedance::bolt::memory::bm
