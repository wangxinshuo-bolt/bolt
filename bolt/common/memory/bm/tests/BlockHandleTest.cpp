/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/tests/BufferManagerTestUtil.h"

namespace bytedance::bolt::memory::bm {

TEST(BlockHandleTest, blockSealPinAndDiscardReclaim) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_block_discard"});

  auto handle = manager.Allocate(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 128,
                      .policy = EvictPolicy::kDiscard,
                      .recoveryFn = nullptr});
  std::memset(handle.MutableData(), 7, handle.Size());
  auto block = handle.Block();
  handle = BufferHandle();

  ASSERT_TRUE(block->IsSealed());
  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 7);
  ASSERT_THROW(
      static_cast<void>(pinned.MutableData()), ::bytedance::bolt::BoltUserError);
  pinned = BufferHandle();

  ASSERT_EQ(manager.Reclaim(128), 128);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
  ASSERT_FALSE(manager.Pin(block).IsValid());
}

TEST(BlockHandleTest, discardReclaimReturnsActualFreedBytes) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_discard_actual_freed"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 256,
                      .policy = EvictPolicy::kDiscard,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 7, bytes); });

  ASSERT_EQ(manager.Reclaim(128), 256);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
  ASSERT_EQ(block->State(), BlockState::kDiscarded);
}

TEST(BlockHandleTest, recomputeBlockCanBePinnedAfterReclaim) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_block_recompute"});

  AllocateOptions options;
  options.tag = MemoryTag::kOperatorState;
  options.size = 64;
  options.policy = EvictPolicy::kRecompute;
  options.recoveryFn = [](DataPtr data, ByteCount bytes) {
    std::memset(data, 42, bytes);
  };

  auto block = manager.AllocatePersistent(
      options, [](DataPtr data, ByteCount bytes) { std::memset(data, 1, bytes); });
  ASSERT_EQ(manager.Reclaim(64), 64);
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
  ASSERT_EQ(block->State(), BlockState::kEvictedRecomputable);

  auto pinned = manager.Pin(block);
  ASSERT_TRUE(pinned.IsValid());
  ASSERT_EQ(pinned.Data()[0], 42);
  ASSERT_EQ(manager.GetMemoryUsage(), 64);
}

TEST(BlockHandleTest, pinnedHandlePreventsDiscardReclaim) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_pinned_handle"});

  auto handle = manager.Allocate(
      AllocateOptions{.tag = MemoryTag::kScanCache,
                      .size = 128,
                      .policy = EvictPolicy::kDiscard,
                      .recoveryFn = nullptr});
  auto block = handle.Block();

  ASSERT_TRUE(block->IsPinned());
  ASSERT_EQ(manager.Reclaim(128), 0);
  ASSERT_EQ(manager.GetMemoryUsage(), 128);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
}

TEST(BlockHandleTest, pinnedForeverBlockIsNeverReclaimed) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_pinned_forever"});

  auto block = manager.AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kInternal,
                      .size = 256,
                      .policy = EvictPolicy::kPinnedForever,
                      .recoveryFn = nullptr},
      [](DataPtr data, ByteCount bytes) { std::memset(data, 3, bytes); });

  ASSERT_FALSE(block->IsPinned());
  ASSERT_EQ(manager.Reclaim(256), 0);
  ASSERT_EQ(manager.GetMemoryUsage(), 256);
  ASSERT_EQ(block->State(), BlockState::kLoaded);
}

// A failed reload must propagate the same error to concurrent waiters instead
// of letting each thread run its own retry. This drives recoveryFn to throw
// exactly once and then counts how many times it actually ran.
TEST(BlockHandleTest, reloadFailurePropagatesAndDoesNotThunder) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_pin_thundering_herd"});

  std::atomic<int> recoveryCalls{0};
  AllocateOptions opts;
  opts.tag = MemoryTag::kHashTable;
  opts.size = 1024;
  opts.policy = EvictPolicy::kRecompute;
  opts.recoveryFn = [&](DataPtr p, ByteCount n) {
    if (recoveryCalls.fetch_add(1) == 0) {
      throw std::runtime_error("inject reload failure");
    }
    std::memset(p, 0, n);
  };

  auto block = manager.AllocatePersistent(
      opts, [](DataPtr p, ByteCount n) { std::memset(p, 0, n); });
  ASSERT_EQ(manager.Reclaim(1024), 1024);
  ASSERT_EQ(block->State(), BlockState::kEvictedRecomputable);

  constexpr int kThreads = 8;
  std::atomic<int> failures{0};
  std::atomic<int> successes{0};
  std::vector<std::thread> ts;
  for (int i = 0; i < kThreads; ++i) {
    ts.emplace_back([&] {
      try {
        auto h = manager.Pin(block);
        if (h.IsValid()) {
          ++successes;
        } else {
          ++failures;
        }
      } catch (...) {
        ++failures;
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }

  // recoveryFn must have run at most a handful of times (one failed attempt
  // plus at most one retry), not once per waiter.
  EXPECT_LE(recoveryCalls.load(), 2);
  EXPECT_GT(failures.load(), 0);
}

// BufferManager destruction must wake any in-flight SpillToDisk worker without
// dereferencing the torn-down manager_. This races the destructor with a
// manual SpillToDisk and asserts there is no crash.
TEST(BlockHandleTest, destroyManagerDuringSpillDoesNotCrash) {
  test::ensureTestSpillService();
  memory::MemoryManager memoryManager;
  auto bm = std::make_unique<BufferManager>(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_destroy_during_spill"});

  auto block = bm->AllocatePersistent(
      AllocateOptions{.tag = MemoryTag::kShuffle,
                      .size = 1 << 16,
                      .policy = EvictPolicy::kSpillToDisk,
                      .recoveryFn = nullptr},
      [](DataPtr p, ByteCount n) { std::memset(p, 7, n); });

  std::thread t([block] { (void)block->SpillToDisk(); });
  bm.reset();
  t.join();
}

} // namespace bytedance::bolt::memory::bm
