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

class FailingSpillRequester final : public SpillRequester {
 public:
  EvictResult SubmitSpill(EvictionNode /*node*/) override {
    ++submitCount;
    return EvictResult{EvictResultKind::kFailed, 0};
  }

  bool WaitForProgress(
      ByteCount /*bytesNeeded*/,
      std::chrono::milliseconds /*timeout*/) override {
    return false;
  }

  int submitCount{0};
};

class FakeBlockHandleBase final : public BlockHandleBase {
 public:
  uint64_t EvictionSequence() const override {
    return sequence;
  }

  uint64_t sequence{0};
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

TEST(SpillTest, snapshotTracksLoadedAndSpilledBytes) {
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_snapshot_loaded_spilled"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 512,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 77, bytes); });

  auto snapshot = manager.Snapshot();
  EXPECT_EQ(snapshot.usedLoadedBytes, 512);
  EXPECT_EQ(snapshot.usedSpilledBytes, 0);

  ASSERT_EQ(manager.Reclaim(512), 512);
  snapshot = manager.Snapshot();
  EXPECT_EQ(snapshot.usedLoadedBytes, 0);
  EXPECT_EQ(snapshot.usedSpilledBytes, 512);
  EXPECT_EQ(block->State(), BlockState::kSpilled);
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
      .SetSpillRequester(requester);

  EvictionNode node;
  ASSERT_TRUE(manager.EvictionQueue().TryPopAnyCandidate(node));
  ASSERT_EQ(manager.EvictionQueue().TryScheduleEvict(node).kind,
            EvictResultKind::kScheduled);
  ASSERT_EQ(manager.EvictionQueue().TryScheduleEvict(node).kind,
            EvictResultKind::kSkipped);
  ASSERT_EQ(requester.submitCount, 1);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
}

TEST(SpillTest, asyncSpillWorkerWritesButOwnerCommitsCompletion) {
  BufferManager::ResetProcessServicesForTesting();
  BufferManagerProcessServicesConfig services;
  services.spill.spillDir = "/tmp/bolt_bm_async_owner_commit";
  services.spill.workerThreadCount = 1;
  services.spill.diskProbeDuration = std::chrono::milliseconds(0);
  services.spill.cleanupOnDestroy = true;
  services.spill.diskIo.backend = DiskIoBackend::kSync;
  services.spill.diskIo.initialQueueDepth = 4;
  services.spill.diskIo.minQueueDepth = 1;
  services.spill.diskIo.maxQueueDepth = 16;
  BufferManager::InitializeProcessServices(std::move(services));

  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_async_owner_commit"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 512,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 4, bytes); });

  EvictionNode node;
  ASSERT_TRUE(manager.EvictionQueue().TryPopAnyCandidate(node));
  ASSERT_EQ(manager.EvictionQueue().TryScheduleEvict(node).kind,
            EvictResultKind::kScheduled);
  ASSERT_TRUE(ProcessSpillService::Instance().WaitForProgress(
      0, std::chrono::seconds(5)));

  ASSERT_EQ(block->State(), BlockState::kSpilling);
  ASSERT_EQ(manager.GetMemoryUsage(), 512);

  ASSERT_EQ(manager.Reclaim(512), 512);
  ASSERT_EQ(block->State(), BlockState::kSpilled);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
}

TEST(SpillTest, asyncSpillCompletionDoesNotKeepBlockAlive) {
  BufferManager::ResetProcessServicesForTesting();
  BufferManagerProcessServicesConfig services;
  services.spill.spillDir = "/tmp/bolt_bm_async_completion_no_block_ref";
  services.spill.workerThreadCount = 1;
  services.spill.diskProbeDuration = std::chrono::milliseconds(0);
  services.spill.cleanupOnDestroy = true;
  services.spill.diskIo.backend = DiskIoBackend::kSync;
  services.spill.diskIo.initialQueueDepth = 4;
  services.spill.diskIo.minQueueDepth = 1;
  services.spill.diskIo.maxQueueDepth = 16;
  BufferManager::InitializeProcessServices(std::move(services));

  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_async_completion_no_block_ref"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 512,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 5, bytes); });
  std::weak_ptr<BlockHandle> weakBlock = block;

  EvictionNode node;
  ASSERT_TRUE(manager.EvictionQueue().TryPopAnyCandidate(node));
  ASSERT_EQ(manager.EvictionQueue().TryScheduleEvict(node).kind,
            EvictResultKind::kScheduled);
  block.reset();

  ASSERT_TRUE(ProcessSpillService::Instance().WaitForProgress(
      0, std::chrono::seconds(5)));
  ASSERT_TRUE(weakBlock.expired());
  ASSERT_EQ(manager.GetMemoryUsage(), 512);

  ASSERT_EQ(manager.Reclaim(512), 512);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
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
  serviceConfig.executionMode = SpillExecutionMode::kOwnerThread;
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

TEST(SpillTest, workerThreadSpillRejectsZeroWorkers) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_test_process_spill_zero_workers";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);

  DiskIoConfig ioConfig;
  ioConfig.backend = DiskIoBackend::kSync;
  ioConfig.initialQueueDepth = 4;
  ioConfig.minQueueDepth = 1;
  ioConfig.maxQueueDepth = 16;
  ProcessDiskIoService::ConfigureDefaultIfNeeded(ioConfig);

  test::RecordingMetricsRegistry metrics;
  ProcessSpillServiceConfig serviceConfig;
  serviceConfig.spillDir = root.string();
  serviceConfig.executionMode = SpillExecutionMode::kWorkerThread;
  serviceConfig.workerThreadCount = 0;
  serviceConfig.cleanupOnDestroy = true;
  serviceConfig.diskProbeDuration = std::chrono::milliseconds(0);
  serviceConfig.metrics = &metrics;
  EXPECT_THROW(
      ProcessSpillService::CreateForTesting(std::move(serviceConfig)),
      BoltUserError);
}

TEST(SpillTest, processSpillServiceRecordsSkippedExpiredSubmit) {
  auto root = std::filesystem::temp_directory_path() /
      "bolt_bm_test_process_spill_skipped";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);

  test::RecordingMetricsRegistry metrics;
  DiskIoConfig ioConfig;
  ioConfig.backend = DiskIoBackend::kSync;
  ioConfig.initialQueueDepth = 4;
  ioConfig.minQueueDepth = 1;
  ioConfig.maxQueueDepth = 16;
  ProcessDiskIoService::ConfigureDefaultIfNeeded(ioConfig);

  ProcessSpillServiceConfig serviceConfig;
  serviceConfig.spillDir = root.string();
  serviceConfig.executionMode = SpillExecutionMode::kWorkerThread;
  serviceConfig.workerThreadCount = 1;
  serviceConfig.cleanupOnDestroy = true;
  serviceConfig.diskProbeDuration = std::chrono::milliseconds(0);
  serviceConfig.metrics = &metrics;
  auto service = ProcessSpillService::CreateForTesting(std::move(serviceConfig));

  EvictionNode node;
  node.cost = EvictionCostClass::kSpill;

  ASSERT_EQ(service->SubmitSpill(node).kind, EvictResultKind::kSkipped);
  EXPECT_EQ(metrics.CounterValue("bm_spill_submit_total"), 1);
  EXPECT_EQ(metrics.CounterValue("bm_spill_skipped_total"), 1);
  EXPECT_EQ(metrics.CounterValue("bm_spill_scheduled_total"), 0);
}

TEST(SpillTest, spillPolicyRequiresExplicitProcessServiceInitialization) {
  BufferManager::ResetProcessServicesForTesting();

  memory::MemoryManager memoryManager;
  BufferManagerConfig config;
  config.poolName = "bm_unconfigured_process_spill_service";
  BufferManager manager(memoryManager, config);

  EXPECT_THROW(
      manager.AllocatePersistent(
          AllocateOptions{.tag = MemoryTag::kHashTable,
                          .size = 1024,
                          .policy = EvictPolicy::kSpillToDisk,
                          .recoveryFn = nullptr},
          [](DataPtr data, ByteCount bytes) { std::memset(data, 55, bytes); }),
      BoltUserError);

  BufferManager::ResetProcessServicesForTesting();
}

TEST(SpillTest, spillPoliciesUseInitializedProcessService) {
  BufferManager::ResetProcessServicesForTesting();

  BufferManagerProcessServicesConfig services;
  services.spill.spillDir =
      test::testSpillDir("bolt_bm_configured_from_manager");
  services.spill.executionMode = SpillExecutionMode::kOwnerThread;
  services.spill.workerThreadCount = 0;
  services.spill.diskProbeDuration = std::chrono::milliseconds(0);
  services.spill.diskIo.backend = DiskIoBackend::kSync;
  services.spill.diskIo.initialQueueDepth = 4;
  services.spill.diskIo.minQueueDepth = 1;
  services.spill.diskIo.maxQueueDepth = 16;
  BufferManager::InitializeProcessServices(std::move(services));

  memory::MemoryManager memoryManager;
  BufferManagerConfig config;
  config.poolName = "bm_process_spill_service";

  {
    BufferManager manager(memoryManager, std::move(config));

    auto block = manager.AllocatePersistent(
        AllocateOptions{.tag = MemoryTag::kHashTable,
                        .size = 1024,
                        .policy = EvictPolicy::kSpillToDisk,
                        .recoveryFn = nullptr},
        [](DataPtr data, ByteCount bytes) { std::memset(data, 55, bytes); });
    ASSERT_EQ(manager.Reclaim(1024), 1024);
    ASSERT_EQ(block->State(), BlockState::kSpilled);
  }

  BufferManager::ResetProcessServicesForTesting();
}

TEST(SpillTest, workerThreadSpillDoesNotFallbackToOwnerThreadOnSubmitFailure) {
  BufferManager::ResetProcessServicesForTesting();

  BufferManagerProcessServicesConfig services;
  services.spill.spillDir = test::testSpillDir("bolt_bm_no_sync_fallback");
  services.spill.executionMode = SpillExecutionMode::kWorkerThread;
  services.spill.workerThreadCount = 1;
  services.spill.diskProbeDuration = std::chrono::milliseconds(0);
  services.spill.diskIo.backend = DiskIoBackend::kSync;
  services.spill.diskIo.initialQueueDepth = 4;
  services.spill.diskIo.minQueueDepth = 1;
  services.spill.diskIo.maxQueueDepth = 16;
  BufferManager::InitializeProcessServices(std::move(services));

  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_no_sync_fallback"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 512,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 6, bytes); });

  FailingSpillRequester requester;
  dynamic_cast<BlockEvictor&>(manager.EvictionQueue())
      .SetSpillRequester(requester);

  ASSERT_EQ(manager.Reclaim(512), 0);
  ASSERT_GE(requester.submitCount, 1);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
  ASSERT_EQ(manager.GetMemoryUsage(), 512);

  BufferManager::ResetProcessServicesForTesting();
}

TEST(SpillTest, multipleBufferManagersShareConfiguredSpillService) {
  BufferManager::ResetProcessServicesForTesting();

  const auto spillDir = test::testSpillDir("bolt_bm_multi_manager_spill_root");
  BufferManagerProcessServicesConfig services;
  services.spill.spillDir = spillDir;
  services.spill.executionMode = SpillExecutionMode::kOwnerThread;
  services.spill.workerThreadCount = 0;
  services.spill.diskProbeDuration = std::chrono::milliseconds(0);
  services.spill.diskIo.backend = DiskIoBackend::kSync;
  services.spill.diskIo.initialQueueDepth = 4;
  services.spill.diskIo.minQueueDepth = 1;
  services.spill.diskIo.maxQueueDepth = 16;
  BufferManager::InitializeProcessServices(std::move(services));

  auto makeConfig = [](const std::string& poolName) {
    BufferManagerConfig config;
    config.poolName = poolName;
    return config;
  };

  memory::MemoryManager memoryManager;
  {
    BufferManager first(memoryManager, makeConfig("bm_task_one"));
    BufferManager second(memoryManager, makeConfig("bm_task_two"));

    auto firstBlock = first.AllocatePersistent(
        AllocateOptions{.tag = MemoryTag::kHashTable,
                        .size = 512,
                        .policy = EvictPolicy::kSpillToDisk,
                        .recoveryFn = nullptr},
        [](DataPtr data, ByteCount bytes) { std::memset(data, 1, bytes); });
    auto secondBlock = second.AllocatePersistent(
        AllocateOptions{.tag = MemoryTag::kShuffle,
                        .size = 512,
                        .policy = EvictPolicy::kSpillToDisk,
                        .recoveryFn = nullptr},
        [](DataPtr data, ByteCount bytes) { std::memset(data, 2, bytes); });

    ASSERT_EQ(first.Reclaim(512), 512);
    ASSERT_EQ(second.Reclaim(512), 512);
    ASSERT_EQ(firstBlock->State(), BlockState::kSpilled);
    ASSERT_EQ(secondBlock->State(), BlockState::kSpilled);
  }

  BufferManager::ResetProcessServicesForTesting();
}

} // namespace bytedance::bolt::memory::bm
