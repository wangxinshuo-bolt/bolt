/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include <limits>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

namespace bytedance::bolt::memory::bm {

TEST(BufferPoolTest, accountedMemoryUsesBoltMemoryPool) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.memoryLimitBytes = 1 << 20,
                          .poolName = "bm_accounted"});

  auto memory = manager.AllocateMemory(MemoryTag::kHashTable, 4096);
  ASSERT_NE(memory->Data(), nullptr);
  ASSERT_EQ(memory->Size(), 4096);
  ASSERT_EQ(manager.GetMemoryUsage(), 4096);
  ASSERT_EQ(manager.GetMemoryUsage(MemoryTag::kHashTable), 4096);
  memory.reset();
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
}

TEST(BufferPoolTest, reservationMoveAndResize) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.memoryLimitBytes = 1 << 20,
                          .poolName = "bm_reservation"});

  auto reservation = manager.ReserveMemory(MemoryTag::kSort, 1024);
  ASSERT_EQ(manager.GetMemoryUsage(), 1024);
  reservation.Resize(2048);
  ASSERT_EQ(manager.GetMemoryUsage(), 2048);
  auto moved = std::move(reservation);
  ASSERT_EQ(reservation.Size(), 0);
  ASSERT_EQ(moved.Size(), 2048);
  moved.Resize(512);
  ASSERT_EQ(manager.GetMemoryUsage(), 512);
  moved.Reset();
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
}

TEST(BufferPoolTest, pinnedLimitRejectsOversizedReservation) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.memoryLimitBytes = 1024,
                          .pinnedLimitBytes = 128,
                          .poolName = "bm_pinned_limit"});

  ASSERT_ANY_THROW(manager.ReserveMemory(
      MemoryTag::kInternal, 256, ReservationKind::kPinned));
  ASSERT_EQ(manager.GetMemoryUsage(), 0);
}

TEST(BufferPoolTest, snapshotTracksEmergencyScratchHeadroom) {
  BufferPool pool(BufferManagerConfig{.memoryLimitBytes = 1024,
                                      .emergencyScratchBytes = 128,
                                      .poolName = "bm_snapshot"});

  auto normal = pool.Reserve(MemoryTag::kHashTable, 800, ReservationKind::kNormal);
  auto snapshot = pool.Snapshot();
  ASSERT_EQ(snapshot.memoryLimitBytes, 1024);
  ASSERT_EQ(snapshot.operatorMemoryLimitBytes, 896);
  ASSERT_EQ(snapshot.usedTotalBytes, 800);
  ASSERT_EQ(snapshot.availableForOperators, 96);

  auto emergency = pool.Reserve(
      MemoryTag::kInternal, 64, ReservationKind::kScratchEmergency);
  snapshot = pool.Snapshot();
  ASSERT_EQ(snapshot.usedScratchBytes, 64);
  ASSERT_EQ(snapshot.usedEmergencyScratchBytes, 64);
}

TEST(BufferPoolTest, rejectsOverflowingReservations) {
  BufferPool pool(BufferManagerConfig{.memoryLimitBytes = 1024,
                                      .pinnedLimitBytes = 512,
                                      .emergencyScratchBytes = 128,
                                      .poolName = "bm_overflow_guard"});

  ASSERT_THROW(
      pool.Reserve(
          MemoryTag::kHashTable,
          std::numeric_limits<ByteCount>::max(),
          ReservationKind::kNormal),
      ::bytedance::bolt::BoltException);
  ASSERT_EQ(pool.GetMemoryUsage(), 0);

  ASSERT_THROW(
      pool.Reserve(
          MemoryTag::kInternal,
          std::numeric_limits<ByteCount>::max(),
          ReservationKind::kPinned),
      ::bytedance::bolt::BoltException);
  ASSERT_EQ(pool.GetMemoryUsage(), 0);

  ASSERT_THROW(
      pool.Reserve(
          MemoryTag::kInternal,
          std::numeric_limits<ByteCount>::max(),
          ReservationKind::kScratchEmergency),
      ::bytedance::bolt::BoltException);
  ASSERT_EQ(pool.GetMemoryUsage(), 0);
}

} // namespace bytedance::bolt::memory::bm
