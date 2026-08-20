#include "bolt/exec/BmHashJoinStorage.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec {

namespace {

const memory::bm::BufferManagerTagStats& findTagStats(
    const std::vector<memory::bm::BufferManagerTagStats>& tagStats,
    memory::bm::MemoryTag tag) {
  for (const auto& stats : tagStats) {
    if (stats.tag == tag) {
      return stats;
    }
  }
  BOLT_FAIL("Missing BufferManager tag stats for {}", toString(tag));
}

} // namespace

std::shared_ptr<BmHashJoinStorage> BmHashJoinStorage::createForJoin(
    std::vector<TypePtr> types,
    uint32_t numKeys,
    bool allowDuplicates,
    bool hasProbedFlag,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize) {
  return std::make_shared<BmHashJoinStorage>(
      std::move(types),
      numKeys,
      allowDuplicates,
      hasProbedFlag,
      std::move(bufferManager),
      rowBlockSize,
      heapBlockSize);
}

BmHashJoinStorage::BmHashJoinStorage(
    std::vector<TypePtr> types,
    uint32_t numKeys,
    bool allowDuplicates,
    bool hasProbedFlag,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize)
    : bufferManager_(std::move(bufferManager)) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  auto nullable = makeNullable(types);
  bm::BmJoinLayoutOptions options{
      .numKeys = numKeys,
      .hasNext = allowDuplicates,
      .hasProbedFlag = hasProbedFlag,
      .hasNormalizedKey = false,
  };
  rows_ = std::make_unique<bm::BmRowContainer>(
      std::move(types),
      std::move(nullable),
      bufferManager_,
      kStorageMemoryTag,
      rowBlockSize,
      heapBlockSize,
      options);
  refreshRowCount();
}

memory::bm::BufferManagerTagStats BmHashJoinStorage::tagStatsSnapshot(
    const memory::bm::BufferManager& bufferManager) {
  return findTagStats(bufferManager.tagStats(), kStorageMemoryTag);
}

std::vector<bool> BmHashJoinStorage::makeNullable(
    const std::vector<TypePtr>& types) {
  return std::vector<bool>(types.size(), true);
}

uint64_t BmHashJoinStorage::counterDelta(uint64_t before, uint64_t after) {
  return after >= before ? after - before : 0;
}

void BmHashJoinStorage::addStatsDelta(
    RuntimeStats& stats,
    const memory::bm::BufferManagerTagStats& before,
    const memory::bm::BufferManagerTagStats& after) {
  const auto writeBytes =
      counterDelta(before.spillWriteBytes, after.spillWriteBytes);
  stats.spilledBytes += writeBytes;
  stats.spillWriteCount +=
      counterDelta(before.spillWriteCount, after.spillWriteCount);
  stats.spillReadCount +=
      counterDelta(before.spillReadCount, after.spillReadCount);
  stats.spillWriteBytes += writeBytes;
  stats.spillReadBytes +=
      counterDelta(before.spillReadBytes, after.spillReadBytes);
  stats.spillPhysicalWriteBytes += counterDelta(
      before.spillPhysicalWriteBytes, after.spillPhysicalWriteBytes);
  stats.spillPhysicalReadBytes += counterDelta(
      before.spillPhysicalReadBytes, after.spillPhysicalReadBytes);
}

void BmHashJoinStorage::refreshRowCount() {
  runtimeStats_.bmRows = rows_->numRows();
}

uint64_t BmHashJoinStorage::activeSegmentRowCount(bm::PartitionId partition)
    const {
  const auto active = rows_->activeSegmentId(partition);
  if (active == bm::kNoSegment) {
    return 0;
  }
  return rows_->activeSegmentNextRowNumber(partition);
}

void BmHashJoinStorage::spillPartition(bm::PartitionId partition) {
  refreshRowCount();
  runtimeStats_.spilledRows = runtimeStats_.spilledRows == 0
      ? runtimeStats_.bmRows
      : runtimeStats_.spilledRows + runtimeStats_.bmRows;
  const auto before = tagStatsSnapshot(*bufferManager_);
  if (rows_->activeSegmentId(partition) != bm::kNoSegment) {
    rows_->sealActivePartitionSegment(partition);
  }
  if (!rows_->segmentsForPartition(partition).empty()) {
    rows_->spillSealedPartition(partition);
  }
  const auto after = tagStatsSnapshot(*bufferManager_);
  runtimeStats_.spillSegments = rows_->segmentsForPartition(partition).size();
  runtimeStats_.spilledSegments = runtimeStats_.spillSegments;
  addStatsDelta(runtimeStats_, before, after);
  runtimeStats_.spillBytes = runtimeStats_.spilledBytes;
}

void BmHashJoinStorage::sealAndSpillActiveSegment(bm::PartitionId partition) {
  refreshRowCount();
  if (rows_->activeSegmentId(partition) == bm::kNoSegment) {
    return;
  }

  const auto rowsCoveredBySpill = runtimeStats_.bmRows;
  const auto segmentsBefore = rows_->segmentsForPartition(partition).size();
  const auto before = tagStatsSnapshot(*bufferManager_);
  rows_->sealActivePartitionSegment(partition);
  rows_->spillSealedPartition(partition);
  const auto after = tagStatsSnapshot(*bufferManager_);
  const auto segmentsAfter = rows_->segmentsForPartition(partition).size();

  runtimeStats_.spilledRows += rowsCoveredBySpill;
  runtimeStats_.spillSegments = segmentsAfter;
  runtimeStats_.spilledSegments +=
      counterDelta(segmentsBefore, segmentsAfter);
  addStatsDelta(runtimeStats_, before, after);
  runtimeStats_.spillBytes = runtimeStats_.spilledBytes;
}

BmHashJoinStorage::LoadedPartition BmHashJoinStorage::loadPartition(
    bm::PartitionId partition) {
  const auto before = tagStatsSnapshot(*bufferManager_);
  LoadedPartition loaded;
  loaded.lease = rows_->acquireRoundLease(partition);
  loaded.rows = rows_->loadPartitionRows(partition);
  ++runtimeStats_.restoreCount;
  const auto after = tagStatsSnapshot(*bufferManager_);
  refreshRowCount();
  addStatsDelta(runtimeStats_, before, after);
  runtimeStats_.spillBytes = runtimeStats_.spilledBytes;
  loaded.stats = runtimeStats_;
  return loaded;
}

} // namespace bytedance::bolt::exec
