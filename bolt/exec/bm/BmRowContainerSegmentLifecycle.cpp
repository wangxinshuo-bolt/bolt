#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <cstring>
#include <span>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

bool blockContainsRow(const BlockRef& block, const char* row) {
  if (block.ptr == nullptr) {
    return false;
  }
  const auto* begin = block.ptr;
  const auto* end = begin + block.used;
  return row >= begin && row < end;
}

void collectReadOnlyEvictBlock(
    BlockRef& block,
    uint64_t& selectedBytes,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& discardBlocks,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& spillBlocks) {
  if (!block.handle.valid()) {
    return;
  }
  BOLT_CHECK_NOT_NULL(block.block);
  auto blockHandle = block.block;
  block.handle = memory::bm::BufferHandle{};
  block.ptr = nullptr;

  if (bufferManager->HasSpillBacking(blockHandle) &&
      !bufferManager->IsDirty(blockHandle)) {
    discardBlocks.push_back(std::move(blockHandle));
  } else {
    spillBlocks.push_back(std::move(blockHandle));
  }
  selectedBytes += block.size;
}

void collectReadOnlyEvictChunkBlocks(
    ChunkData& chunk,
    uint64_t& selectedBytes,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& discardBlocks,
    std::vector<std::shared_ptr<memory::bm::BlockHandle>>& spillBlocks) {
  discardBlocks.reserve(discardBlocks.size() + 1 + chunk.heapBlocks.size());
  spillBlocks.reserve(spillBlocks.size() + 1 + chunk.heapBlocks.size());
  // Keep ReadOnlyWindow eviction chunk-granular. Rebased StringView pointers
  // make the row block dirty, so dirty row blocks are written back with heapBases
  // tracking the same heap address space. Clean blocks with existing backing can
  // still be discarded.
  collectReadOnlyEvictBlock(
      chunk.rowBlock, selectedBytes, bufferManager, discardBlocks, spillBlocks);
  for (auto& block : chunk.heapBlocks) {
    collectReadOnlyEvictBlock(
        block, selectedBytes, bufferManager, discardBlocks, spillBlocks);
  }
}

} // namespace

SegmentId BmRowContainer::spillActiveSegment() {
  checkNoLiveLeaseForPartition(kDefaultPartition);
  const auto segment = segments_.spillActiveSegment();
  invalidatePartitionRows(kDefaultPartition);
  return segment;
}

SegmentId BmRowContainer::spillActivePartitionSegment(PartitionId partition) {
  checkNoLiveLeaseForPartition(partition);
  const auto segment = segments_.spillActivePartitionSegment(partition);
  invalidatePartitionRows(partition);
  return segment;
}

SegmentId BmRowContainer::sealActivePartitionSegment(PartitionId partition) {
  checkNoLiveLeaseForPartition(partition);
  return segments_.sealActivePartitionSegment(partition);
}

SegmentId BmRowContainer::spillSealedPartition(PartitionId partition) {
  checkNoLiveLeaseForPartition(partition);
  const auto segment = segments_.spillSealedPartition(partition);
  invalidatePartitionRows(partition);
  return segment;
}

std::vector<char*> BmRowContainer::loadPartitionRows(PartitionId partition) {
  const auto& segments = segments_.segmentsForPartition(partition);
  auto rows = loadAllRows({segments.data(), segments.size()});
  for (auto segment : segments) {
    auto& data = segments_.segmentData(segment);
    data.meta.generation = partitionLeaseStates_[partition]->generation;
    for (auto& chunkPtr : data.chunks) {
      auto& chunk = *chunkPtr;
      if (chunk.consumed || !chunk.rowBlock.handle.valid()) {
        continue;
      }
      bufferManager_->MarkDirty(chunk.rowBlock.block);
      auto* row = chunk.rowBlock.ptr;
      for (uint32_t i = 0; i < chunk.meta.rowCount; ++i) {
        layout_.resetJoinRuntimeMetadata(row);
        row += segments_.rowStride();
      }
    }
  }
  return rows;
}

BmRoundLease BmRowContainer::acquireRoundLease(PartitionId partition) {
  BOLT_CHECK_LT(partition, kMaxPartitions);
  auto& state = partitionLeaseStates_[partition];
  BOLT_CHECK_EQ(
      state->activeLeaseCount,
      0,
      "Partition {} already has a live BM round lease",
      partition);
  ++state->activeLeaseCount;
  return BmRoundLease(state, state->generation);
}

void BmRowContainer::releaseRoundLease(BmRoundLease& lease) {
  if (!lease.active()) {
    return;
  }
  auto state = lease.state_;
  BOLT_CHECK_NOT_NULL(state);
  BOLT_CHECK(state->ownerAlive, "Lease owner has already been destroyed");
  BOLT_CHECK(state->owner == this, "Lease does not belong to this container");
  const auto partition = lease.partition_;
  BOLT_CHECK_LT(partition, kMaxPartitions);
  BOLT_CHECK(
      state.get() == partitionLeaseStates_[partition].get(),
      "Lease state does not match partition {}",
      partition);
  BOLT_CHECK_EQ(
      lease.generation_,
      state->generation,
      "Lease generation {} is stale for partition {} current generation {}",
      lease.generation_,
      partition,
      state->generation);
  BOLT_CHECK_GT(state->activeLeaseCount, 0);
  --state->activeLeaseCount;
  lease.state_.reset();
  invalidatePartitionRows(partition);
  lease.partition_ = kDefaultPartition;
  lease.generation_ = 0;
}

void BmRowContainer::releaseSegment(SegmentId segment) {
  checkNoLiveLeaseForSegment(segment);
  segments_.releaseSegment(segment);
}

void BmRowContainer::releaseSegments(folly::Range<const SegmentId*> segments) {
  checkNoLiveLeaseForSegments(segments);
  for (auto segment : segments) {
    releaseSegment(segment);
  }
}

void BmRowContainer::releaseChunk(SegmentId segment, ChunkId chunk) {
  checkNoLiveLeaseForChunk(segment, chunk);
  auto& segmentData = segments_.segmentData(segment);
  BOLT_CHECK_LT(chunk, segmentData.chunks.size());
  segments_.releaseChunkBlocks(*segmentData.chunks[chunk]);
}

void BmRowContainer::popFrontRows(uint64_t rowCount) {
  checkNoLiveLeaseForPopFrontRows(rowCount);
  segments_.popFrontRows(rowCount);
}

uint64_t BmRowContainer::evictReadOnlyLoadedChunks(
    folly::Range<const std::pair<SegmentId, ChunkId>*> chunks,
    uint64_t targetBytes) {
  if (chunks.empty() || targetBytes == 0) {
    return 0;
  }

  uint64_t selectedBytes = 0;
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> discardBlocks;
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> spillBlocks;

  for (const auto& [segment, chunk] : chunks) {
    if (selectedBytes >= targetBytes) {
      break;
    }
    checkNoLiveLeaseForChunk(segment, chunk);
    auto& segmentData = segments_.segmentData(segment);
    BOLT_CHECK(
        segmentData.meta.state != SegmentState::kActiveResident,
        "Cannot evict loaded blocks from active segment {}",
        segment);
    BOLT_CHECK_LT(chunk, segmentData.chunks.size());
    auto& chunkData = *segmentData.chunks[chunk];
    BOLT_CHECK(
        !chunkData.consumed,
        "Cannot evict consumed chunk {} in segment {}",
        chunk,
        segment);

    collectReadOnlyEvictChunkBlocks(
        chunkData,
        selectedBytes,
        bufferManager_,
        discardBlocks,
        spillBlocks);
  }

  uint64_t reclaimed = 0;
  if (!discardBlocks.empty()) {
    reclaimed += bufferManager_->DiscardCleanResidentBlocks(
        std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
            discardBlocks.data(), discardBlocks.size()));
  }
  if (!spillBlocks.empty()) {
    for (const auto& block : spillBlocks) {
      reclaimed += block->size();
    }
    bufferManager_->SpillBlocks(
        std::span<const std::shared_ptr<memory::bm::BlockHandle>>(
            spillBlocks.data(), spillBlocks.size()));
  }
  return reclaimed;
}

SegmentState BmRowContainer::segmentState(SegmentId segment) const {
  return segments_.segmentState(segment);
}

const std::vector<SegmentId>& BmRowContainer::segmentsForPartition(
    PartitionId partition) const {
  return segments_.segmentsForPartition(partition);
}

SegmentId BmRowContainer::activeSegmentId(PartitionId partition) const {
  return segments_.activeSegmentId(partition);
}

RowNumber BmRowContainer::activeSegmentNextRowNumber(
    PartitionId partition) const {
  return segments_.activeSegmentNextRowNumber(partition);
}

uint32_t BmRowContainer::rowSize() const {
  return layout_.rowSize();
}

void BmRowContainer::copyRowWithDeepColumns(
    const char* row,
    folly::Range<const int32_t*> columns,
    std::vector<char>& rowCopy,
    std::vector<char>& variableCopy) const {
  BOLT_CHECK_NOT_NULL(row);
  rowCopy.resize(layout_.rowSize());
  std::memcpy(rowCopy.data(), row, rowCopy.size());

  uint64_t variableBytes = 0;
  for (auto columnIndex : columns) {
    const auto& column = layout_.column(columnIndex);
    const auto kind = column.type->kind();
    if (kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) {
      continue;
    }
    if (layout_.isNull(row, columnIndex)) {
      continue;
    }
    const auto& value =
        *reinterpret_cast<const StringView*>(row + column.offset);
    if (!value.isInline()) {
      variableBytes += value.size();
    }
  }

  variableCopy.resize(variableBytes);
  auto* variable = variableCopy.data();
  for (auto columnIndex : columns) {
    const auto& column = layout_.column(columnIndex);
    const auto kind = column.type->kind();
    if (kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) {
      continue;
    }
    if (layout_.isNull(row, columnIndex)) {
      continue;
    }
    auto* target =
        reinterpret_cast<StringView*>(rowCopy.data() + column.offset);
    if (target->isInline()) {
      continue;
    }
    const auto size = target->size();
    std::memcpy(variable, target->data(), size);
    *target = StringView(variable, size);
    variable += size;
  }
}

int64_t BmRowContainer::numRows() const {
  return segments_.numRows();
}

void BmRowContainer::checkRowPointerReadable(const char* row) const {
  BOLT_CHECK_NOT_NULL(row);
  for (auto segment : segments_.allSegmentIds()) {
    const auto& data = segments_.segmentData(segment);
    for (const auto& chunkPtr : data.chunks) {
      const auto& chunk = *chunkPtr;
      if (chunk.consumed || !blockContainsRow(chunk.rowBlock, row)) {
        continue;
      }
      const auto rowOffset = static_cast<uintptr_t>(row - chunk.rowBlock.ptr);
      BOLT_CHECK_EQ(
          rowOffset % segments_.rowStride(),
          0,
          "BM row pointer is not row-aligned");
      if (data.meta.partitionId.has_value()) {
        const auto partition = *data.meta.partitionId;
        BOLT_CHECK_EQ(
            data.meta.generation,
            partitionLeaseStates_[partition]->generation,
            "BM row pointer belongs to stale partition epoch");
      }
      return;
    }
  }
  BOLT_FAIL("BM row pointer is not reachable in the current epoch");
}

void BmRowContainer::checkNoLiveLeaseForPartition(PartitionId partition) const {
  BOLT_CHECK_LT(partition, kMaxPartitions);
  BOLT_CHECK_EQ(
      partitionLeaseStates_[partition]->activeLeaseCount,
      0,
      "Partition {} has a live BM round lease",
      partition);
}

void BmRowContainer::checkNoLiveLeaseForSegment(SegmentId segment) const {
  const auto partition = segments_.partitionForSegment(segment);
  if (partition.has_value()) {
    checkNoLiveLeaseForPartition(*partition);
    return;
  }
  for (PartitionId partitionId = 0; partitionId < kMaxPartitions;
       ++partitionId) {
    checkNoLiveLeaseForPartition(partitionId);
  }
}

void BmRowContainer::checkNoLiveLeaseForSegments(
    folly::Range<const SegmentId*> segments) const {
  for (auto segment : segments) {
    checkNoLiveLeaseForSegment(segment);
  }
}

void BmRowContainer::checkNoLiveLeaseForChunk(
    SegmentId segment,
    ChunkId /*chunk*/) const {
  checkNoLiveLeaseForSegment(segment);
}

void BmRowContainer::checkNoLiveLeaseForPopFrontRows(uint64_t rowCount) const {
  if (rowCount == 0) {
    return;
  }
  for (PartitionId partition = 0; partition < kMaxPartitions; ++partition) {
    checkNoLiveLeaseForPartition(partition);
  }
}

void BmRowContainer::invalidatePartitionRows(PartitionId partition) {
  BOLT_CHECK_LT(partition, kMaxPartitions);
  ++partitionLeaseStates_[partition]->generation;
}

} // namespace bytedance::bolt::exec::bm
