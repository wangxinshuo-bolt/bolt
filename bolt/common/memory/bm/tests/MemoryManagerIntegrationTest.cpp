/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"

namespace bytedance::bolt::memory::bm {

TEST(MemoryManagerIntegrationTest, ownsOptionalBufferManager) {
  memory::MemoryManager::Options options;
  options.enableBufferManager = true;
  memory::MemoryManager memoryManager(options);

  ASSERT_NE(memoryManager.bufferManager(), nullptr);
  auto memory = memoryManager.bufferManager()->AllocateMemory(
      MemoryTag::kInternal, 256);
  ASSERT_EQ(memoryManager.bufferManager()->GetMemoryUsage(), 256);
}

TEST(MemoryManagerIntegrationTest, disabledByDefault) {
  memory::MemoryManager memoryManager;
  ASSERT_EQ(memoryManager.bufferManager(), nullptr);
}

} // namespace bytedance::bolt::memory::bm
