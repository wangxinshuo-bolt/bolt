#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

BmRowContainer::BmRowContainer(
    std::vector<TypePtr> types,
    std::vector<bool> nullable,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize,
    BmJoinLayoutOptions joinOptions)
    : types_(std::move(types)),
      layout_(types_, nullable, rowBlockSize, joinOptions),
      bufferManager_(std::move(bufferManager)),
      segments_(
          bufferManager_,
          tag,
          &layout_,
          rowBlockSize,
          heapBlockSize),
      blockLoader_(bufferManager_, &layout_, &segments_),
      rowCopier_(&types_, &layout_, &segments_),
      partitionGenerations_(kMaxPartitions, 0),
      partitionLeaseCounts_(kMaxPartitions, 0) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
}

RowWriteContext BmRowContainer::appendRow(PartitionId partition) {
  checkNoLiveLeaseForPartition(partition);
  auto& segment = segments_.activeSegment(partition);
  segment.meta.generation = partitionGenerations_[partition];
  auto* row = segments_.newRowInSegment(segment);
  BOLT_DCHECK_NOT_NULL(segment.writeCursor.chunk);
  auto& chunk = *segment.writeCursor.chunk;
  return RowWriteContext(&segment, &chunk, row);
}

} // namespace bytedance::bolt::exec::bm
