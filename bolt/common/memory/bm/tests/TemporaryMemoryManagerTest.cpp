/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

namespace bytedance::bolt::memory::bm {

TEST(TemporaryMemoryManagerTest, givesAdvisoryBudget) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.memoryLimitBytes = 1024,
                          .poolName = "bm_temp_memory"});

  auto state = manager.TempMemory().Register(TempMemoryRegisterOptions{
      .queryId = "q",
      .operatorId = "op",
      .operatorName = "sort",
      .tag = MemoryTag::kSort,
      .estimatedRemainingBytes = 512,
      .minimumReservationBytes = 128});
  auto decision = state->Decision();
  ASSERT_GT(decision.reservation, 0);
  ASSERT_FALSE(decision.shouldExternalize);

  auto reservation = manager.ReserveMemory(MemoryTag::kHashTable, 960);
  manager.TempMemory().RecomputeBudgets();
  decision = state->Decision();
  ASSERT_TRUE(decision.shouldExternalize);
}

TEST(TemporaryMemoryManagerTest, recomputesBudgetAfterRemainingBytesUpdate) {
  memory::MemoryManager memoryManager;
  BufferManager manager(
      memoryManager,
      BufferManagerConfig{.memoryLimitBytes = 1024,
                          .poolName = "bm_temp_update"});

  auto first = manager.TempMemory().Register(TempMemoryRegisterOptions{
      .queryId = "q",
      .operatorId = "op1",
      .operatorName = "hash",
      .tag = MemoryTag::kHashTable,
      .estimatedRemainingBytes = 1024,
      .minimumReservationBytes = 64});
  auto second = manager.TempMemory().Register(TempMemoryRegisterOptions{
      .queryId = "q",
      .operatorId = "op2",
      .operatorName = "sort",
      .tag = MemoryTag::kSort,
      .estimatedRemainingBytes = 1024,
      .minimumReservationBytes = 64});

  ASSERT_EQ(first->Decision().reservation, 512);
  ASSERT_EQ(second->Decision().reservation, 512);

  first->UpdateRemaining(128);
  ASSERT_EQ(first->Decision().reservation, 128);
  ASSERT_EQ(second->Decision().reservation, 512);
}

} // namespace bytedance::bolt::memory::bm
