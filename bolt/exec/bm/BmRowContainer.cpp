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
      partitionLeaseStates_(kMaxPartitions) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  for (PartitionId partition = 0; partition < kMaxPartitions; ++partition) {
    auto state = std::make_shared<BmRoundLeaseState>();
    state->owner = this;
    state->partition = partition;
    partitionLeaseStates_[partition] = std::move(state);
  }
}

BmRowContainer::~BmRowContainer() {
  for (auto& state : partitionLeaseStates_) {
    if (state != nullptr) {
      state->ownerAlive = false;
      state->owner = nullptr;
    }
  }
}

RowWriteContext BmRowContainer::appendRow(PartitionId partition) {
  checkNoLiveLeaseForPartition(partition);
  auto& segment = segments_.activeSegment(partition);
  segment.meta.generation = partitionLeaseStates_[partition]->generation;
  auto* row = segments_.newRowInSegment(segment);
  BOLT_DCHECK_NOT_NULL(segment.writeCursor.chunk);
  auto& chunk = *segment.writeCursor.chunk;
  return RowWriteContext(&segment, &chunk, row);
}

} // namespace bytedance::bolt::exec::bm
