#pragma once

#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/bm/BmRoundLease.h"
#include "bolt/exec/bm/BmRowContainer.h"

#include <memory>
#include <vector>

namespace bytedance::bolt::exec {

class BmHashJoinStorage {
 public:
  struct RuntimeStats {
    uint64_t bmRows{0};
    uint64_t spillBytes{0};
    uint64_t spillSegments{0};
    uint64_t restoreCount{0};
    uint64_t spilledRows{0};
    uint64_t spilledBytes{0};
    uint64_t spilledSegments{0};
    uint64_t spillWriteCount{0};
    uint64_t spillReadCount{0};
    uint64_t spillWriteBytes{0};
    uint64_t spillReadBytes{0};
    uint64_t spillPhysicalWriteBytes{0};
    uint64_t spillPhysicalReadBytes{0};
  };

  struct LoadedPartition {
    std::vector<char*> rows;
    bm::BmRoundLease lease;
    RuntimeStats stats;
  };

  static std::shared_ptr<BmHashJoinStorage> createForJoin(
      std::vector<TypePtr> types,
      uint32_t numKeys,
      bool allowDuplicates,
      bool hasProbedFlag,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      uint32_t rowBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      uint32_t heapBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)));

  BmHashJoinStorage(
      std::vector<TypePtr> types,
      uint32_t numKeys,
      bool allowDuplicates,
      bool hasProbedFlag,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      uint32_t rowBlockSize,
      uint32_t heapBlockSize);

  bm::BmRowContainer& rows() {
    return *rows_;
  }

  const bm::BmRowContainer& rows() const {
    return *rows_;
  }

  const std::shared_ptr<memory::bm::BufferManager>& bufferManager() const {
    return bufferManager_;
  }

  const RuntimeStats& runtimeStats() const {
    return runtimeStats_;
  }

  uint64_t activeSegmentRowCount(
      bm::PartitionId partition = bm::kDefaultPartition) const;

  void refreshRowCount();
  void spillPartition(bm::PartitionId partition = bm::kDefaultPartition);
  void sealAndSpillActiveSegment(
      bm::PartitionId partition = bm::kDefaultPartition);
  LoadedPartition loadPartition(
      bm::PartitionId partition = bm::kDefaultPartition);

 private:
  static constexpr memory::bm::MemoryTag kStorageMemoryTag =
      memory::bm::MemoryTag::kHashJoin;

  static memory::bm::BufferManagerTagStats tagStatsSnapshot(
      const memory::bm::BufferManager& bufferManager);
  static std::vector<bool> makeNullable(const std::vector<TypePtr>& types);
  static uint64_t counterDelta(uint64_t before, uint64_t after);
  static void addStatsDelta(
      RuntimeStats& stats,
      const memory::bm::BufferManagerTagStats& before,
      const memory::bm::BufferManagerTagStats& after);

  // Keep BufferManager before rows_ so its blocks outlive row-container teardown.
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  std::unique_ptr<bm::BmRowContainer> rows_;
  RuntimeStats runtimeStats_;
};

} // namespace bytedance::bolt::exec
