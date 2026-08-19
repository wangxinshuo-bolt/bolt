#pragma once

#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/MemoryTag.h"
#include "bolt/exec/bm/BmBatchAppend.h"
#include "bolt/exec/bm/BmRowBlockLoader.h"
#include "bolt/exec/bm/BmRowContainerPublicTypes.h"
#include "bolt/exec/bm/BmRowContainerRead.h"
#include "bolt/exec/bm/BmRowCopier.h"
#include "bolt/exec/bm/BmRowLayout.h"
#include "bolt/exec/bm/BmRowWriteContext.h"
#include "bolt/exec/bm/BmSegmentCollection.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"

#include <folly/Range.h>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace bytedance::bolt::exec::bm {

struct BmRowContainerTestPeer;

class BmRowContainer {
 public:
  BmRowContainer(
      std::vector<TypePtr> types,
      std::vector<bool> nullable,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag,
      uint32_t rowBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      uint32_t heapBlockSize = static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      BmJoinLayoutOptions joinOptions = {});

  // Allocates one row in the active segment for partition. The caller must fill
  // columns with store() before treating the row as complete.
  RowWriteContext appendRow(PartitionId partition = kDefaultPartition);

  // Appends all rows from input through a batch-only writer. This path reserves
  // contiguous row ranges and stores columns by range; appendRow() + store()
  // keeps its separate row-wise path.
  void appendBatch(
      const RowVectorPtr& input,
      PartitionId partition = kDefaultPartition,
      std::vector<char*>* rows = nullptr,
      BmBatchStringStoreMode stringStoreMode = BmBatchStringStoreMode::kCopy);

  void appendBatchSelected(
      const RowVectorPtr& input,
      const SelectivityVector& selectedRows,
      PartitionId partition = kDefaultPartition,
      std::vector<char*>* rows = nullptr,
      BmBatchStringStoreMode stringStoreMode = BmBatchStringStoreMode::kCopy);

  FOLLY_ALWAYS_INLINE void store(
      RowWriteContext& context,
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      int32_t column) {
    storeValue(decoded, sourceIndex, context, column);
  }

  int32_t compare(
      const char* left,
      const char* right,
      int32_t column,
      CompareFlags flags = {});

  int32_t compare(
      const char* left,
      const char* right,
      int32_t leftColumn,
      int32_t rightColumn,
      CompareFlags flags = {});

  int32_t compareRows(
      const char* left,
      const char* right,
      const std::vector<CompareFlags>& flags = {});

  FOLLY_ALWAYS_INLINE bool isNull(const char* row, int32_t column) const {
    return layout_.isNull(row, column);
  }

  FOLLY_ALWAYS_INLINE char* next(const char* row) const {
    const auto& runtime = layout_.joinRuntimeLayout();
    BOLT_DCHECK(runtime.hasNext);
    return *reinterpret_cast<char* const*>(row + runtime.nextOffset);
  }

  FOLLY_ALWAYS_INLINE void setNext(char* row, char* nextRow) const {
    const auto& runtime = layout_.joinRuntimeLayout();
    BOLT_DCHECK(runtime.hasNext);
    *reinterpret_cast<char**>(row + runtime.nextOffset) = nextRow;
  }

  FOLLY_ALWAYS_INLINE bool probed(const char* row) const {
    const auto& runtime = layout_.joinRuntimeLayout();
    BOLT_DCHECK(runtime.hasProbedFlag);
    return *reinterpret_cast<const bool*>(row + runtime.probedOffset);
  }

  FOLLY_ALWAYS_INLINE void setProbed(char* row, bool value) const {
    const auto& runtime = layout_.joinRuntimeLayout();
    BOLT_DCHECK(runtime.hasProbedFlag);
    *reinterpret_cast<bool*>(row + runtime.probedOffset) = value;
  }

  FOLLY_ALWAYS_INLINE uint64_t normalizedKey(const char* row) const {
    const auto& runtime = layout_.joinRuntimeLayout();
    BOLT_DCHECK(runtime.hasNormalizedKey);
    return *reinterpret_cast<const uint64_t*>(row + runtime.normalizedKeyOffset);
  }

  FOLLY_ALWAYS_INLINE void setNormalizedKey(char* row, uint64_t value) const {
    const auto& runtime = layout_.joinRuntimeLayout();
    BOLT_DCHECK(runtime.hasNormalizedKey);
    *reinterpret_cast<uint64_t*>(row + runtime.normalizedKeyOffset) = value;
  }

  FOLLY_ALWAYS_INLINE void resetJoinRuntimeMetadata(char* row) const {
    layout_.resetJoinRuntimeMetadata(row);
  }

  void extractColumnResident(
      const char* const* rows,
      int32_t numRows,
      int32_t column,
      const VectorPtr& result,
      bool exactSize = false) {
    extractColumnResident(rows, numRows, column, 0, result, exactSize);
  }

  void extractColumnResident(
      const char* const* rows,
      int32_t numRows,
      int32_t column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  void extractColumnResident(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  void extractNullsResident(
      const char* const* rows,
      int32_t numRows,
      int32_t column,
      const BufferPtr& result);

  SegmentId spillActiveSegment();
  SegmentId spillActivePartitionSegment(PartitionId partition);

  // Materializes resident rows in the supplied order into a new
  // finalized/flushed segment. The returned SegmentId can be scanned through
  // MergeReadSession.
  //
  // This implementation copies all rows into a second segment before flushing.
  // It gives merge readers sequential scan locality, but it can temporarily
  // double memory for the reordered rows. Use it only while the caller can
  // tolerate that peak; large memory-pressure paths should eventually switch to
  // segmented materialization that flushes smaller ordered pieces
  // incrementally.
  SegmentId finalizeReorderedSegment(folly::Range<char* const*> sortedRows);

  // Conservative estimate for whether all blocks in segments can be bulk
  // loaded now. Only blocks with active BufferHandle are treated as loaded;
  // unpinned resident blocks are counted as reloadable because MaybeReserve()
  // may reclaim them while probing capacity. This is a hint only:
  // BulkReadSession::load() still performs the actual reservation and may throw
  // if memory changes.
  bool canBulkRead(folly::Range<const SegmentId*> segments) const;

  BulkReadSession beginBulkReadSegments(
      folly::Range<const SegmentId*> segments);

  ReadOnlyWindowReadSession beginReadOnlyWindowReadSegments(
      folly::Range<const SegmentId*> segments);

  // Creates a session for scanning/comparing physically ordered segments. If
  // releaseAfterRead is true, each cursor drops chunk blocks after passing
  // them.
  MergeReadSession beginMergeReadSegments(
      folly::Range<const SegmentId*> segments,
      bool releaseAfterRead = true);

  void releaseSegment(SegmentId segment);
  void releaseSegments(folly::Range<const SegmentId*> segments);
  void releaseChunk(SegmentId segment, ChunkId chunk);
  void popFrontRows(uint64_t rowCount);

  SegmentState segmentState(SegmentId segment) const;
  const std::vector<SegmentId>& segmentsForPartition(
      PartitionId partition) const;
  SegmentId activeSegmentId(PartitionId partition = kDefaultPartition) const;
  RowNumber activeSegmentNextRowNumber(
      PartitionId partition = kDefaultPartition) const;
  uint32_t rowSize() const;
  void copyRowWithDeepColumns(
      const char* row,
      folly::Range<const int32_t*> columns,
      std::vector<char>& rowCopy,
      std::vector<char>& variableCopy) const;
  int64_t numRows() const;

 private:
  friend class BmRowLayout;
  friend struct BmRowContainerTestPeer;

  int32_t compareNonNull(
      const char* left,
      const char* right,
      int32_t leftColumn,
      int32_t rightColumn) const;
  void storeValue(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriteContext& context,
      int32_t column);
  static ColumnStorePlan::StoreValueFn storeFnFor(TypeKind kind, bool nullable);
  template <TypeKind Kind>
  static ColumnStorePlan::StoreValueFn storeNoNullsFn();
  template <TypeKind Kind>
  static ColumnStorePlan::StoreValueFn storeWithNullsFn();
  template <TypeKind Kind>
  void storeNoNullsTyped(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriteContext& context,
      const ColumnStorePlan& column);
  template <TypeKind Kind>
  void storeWithNullsTyped(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriteContext& context,
      const ColumnStorePlan& column);
  template <TypeKind Kind>
  void storeNonNullValueTyped(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriteContext& context,
      const ColumnStorePlan& column);
  template <TypeKind Kind>
  void storeFixedColumnBatchRangesNoNullsTyped(
      const DecodedVector& decoded,
      folly::Range<const BatchAppendRange*> ranges,
      const ColumnStorePlan& column);
  template <TypeKind Kind>
  void storeFixedColumnBatchRangesWithNullsTyped(
      const DecodedVector& decoded,
      folly::Range<const BatchAppendRange*> ranges,
      const ColumnStorePlan& column);
  void storeStringColumnBatchRanges(
      const DecodedVector& decoded,
      folly::Range<const BatchAppendRange*> ranges,
      const ColumnStorePlan& column,
      BmBatchStringStoreMode stringStoreMode);
  static std::vector<BatchAppendRange> selectedRanges(
      const std::vector<BatchAppendRange>& reservedRanges,
      const SelectivityVector& selectedRows,
      std::vector<char*>* rows,
      uint32_t rowStride);
  template <TypeKind Kind>
  void extractColumnTyped(
      const char* const* rows,
      int32_t numRows,
      const ColumnLayout& column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize) const;
  template <TypeKind Kind>
  void extractColumnByRowNumbersTyped(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      const ColumnLayout& column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize) const;
  void ensureSegmentsLoaded(folly::Range<const SegmentId*> segments);
  void ensureChunksLoaded(folly::Range<ChunkData* const*> chunks);
  void ensureChunkLoaded(ChunkData& chunk);
  std::vector<char*> loadAllRows(folly::Range<const SegmentId*> segments);
  std::vector<RowId> listRowIdsForSegments(
      folly::Range<const SegmentId*> segments) const;
  uint64_t evictReadOnlyLoadedChunks(
      folly::Range<const std::pair<SegmentId, ChunkId>*> chunks,
      uint64_t targetBytes);

  uint64_t unloadedBytes(folly::Range<const SegmentId*> segments) const;

  friend class BulkReadSession;
  friend class ReadOnlyWindowReadSession;
  friend class MergeReadSession;

  FOLLY_ALWAYS_INLINE void validateSegments(
      folly::Range<const SegmentId*> segments) const {
    for (auto segment : segments) {
      (void)segments_.segmentData(segment);
    }
  }

  std::vector<TypePtr> types_;
  BmRowLayout layout_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  BmSegmentCollection segments_;
  BmRowBlockLoader blockLoader_;
  BmRowCopier rowCopier_;
};

} // namespace bytedance::bolt::exec::bm
