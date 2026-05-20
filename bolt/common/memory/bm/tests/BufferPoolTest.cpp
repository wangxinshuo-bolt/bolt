/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

namespace bytedance::bolt::memory::bm {

TEST(BufferPoolTest, accountedMemoryUsesBoltMemoryPool) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_accounted"});

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
      BufferManagerConfig{.poolName = "bm_reservation"});

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

TEST(BufferPoolTest, pinnedReservationIsUsageOnly) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.poolName = "bm_pinned_usage"});

  auto reservation = manager.ReserveMemory(
      MemoryTag::kInternal, 256, ReservationKind::kPinned);
  ASSERT_EQ(manager.GetMemoryUsage(), 256);
  ASSERT_EQ(manager.Snapshot().usedPinnedBytes, 256);
}

TEST(BufferPoolTest, snapshotTracksScratchUsage) {
  BufferPool pool(BufferManagerConfig{.poolName = "bm_snapshot"});

  auto normal = pool.Reserve(MemoryTag::kHashTable, 800, ReservationKind::kNormal);
  auto snapshot = pool.Snapshot();
  ASSERT_EQ(snapshot.usedTotalBytes, 800);

  auto emergency = pool.Reserve(
      MemoryTag::kInternal, 64, ReservationKind::kScratchEmergency);
  snapshot = pool.Snapshot();
  ASSERT_EQ(snapshot.usedScratchBytes, 64);
  ASSERT_EQ(snapshot.usedEmergencyScratchBytes, 64);
}

} // namespace bytedance::bolt::memory::bm
