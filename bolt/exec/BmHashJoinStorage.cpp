#include "bolt/exec/BmHashJoinStorage.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec {

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
      memory::bm::MemoryTag::kHashBuild,
      rowBlockSize,
      heapBlockSize,
      options);
  refreshRowCount();
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
    const memory::bm::BufferManagerStats& before,
    const memory::bm::BufferManagerStats& after) {
  stats.spilledBytes = std::max(stats.spilledBytes, after.spilledBytes);
  stats.spillWriteCount +=
      counterDelta(before.spillWriteCount, after.spillWriteCount);
  stats.spillReadCount +=
      counterDelta(before.spillReadCount, after.spillReadCount);
  stats.spillWriteBytes +=
      counterDelta(before.spillWriteBytes, after.spillWriteBytes);
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

void BmHashJoinStorage::spillPartition(bm::PartitionId partition) {
  refreshRowCount();
  runtimeStats_.spilledRows = runtimeStats_.bmRows;
  const auto before = bufferManager_->stats();
  if (rows_->activeSegmentId(partition) != bm::kNoSegment) {
    rows_->sealActivePartitionSegment(partition);
  }
  if (!rows_->segmentsForPartition(partition).empty()) {
    rows_->spillSealedPartition(partition);
  }
  const auto after = bufferManager_->stats();
  runtimeStats_.spillSegments = rows_->segmentsForPartition(partition).size();
  runtimeStats_.spilledSegments = runtimeStats_.spillSegments;
  addStatsDelta(runtimeStats_, before, after);
  runtimeStats_.spillBytes = runtimeStats_.spilledBytes;
}

BmHashJoinStorage::LoadedPartition BmHashJoinStorage::loadPartition(
    bm::PartitionId partition) {
  const auto before = bufferManager_->stats();
  LoadedPartition loaded;
  loaded.rows = rows_->loadPartitionRows(partition);
  loaded.lease = rows_->acquireRoundLease(partition);
  ++runtimeStats_.restoreCount;
  const auto after = bufferManager_->stats();
  refreshRowCount();
  addStatsDelta(runtimeStats_, before, after);
  runtimeStats_.spillBytes = runtimeStats_.spilledBytes;
  loaded.stats = runtimeStats_;
  return loaded;
}

} // namespace bytedance::bolt::exec
