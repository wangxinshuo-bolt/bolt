#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::exec::bm {

std::vector<BatchAppendRange> BmRowContainer::selectedRanges(
    const std::vector<BatchAppendRange>& reservedRanges,
    const SelectivityVector& selectedRows,
    std::vector<char*>* rows,
    uint32_t rowStride) {
  std::vector<BatchAppendRange> selected;
  if (!selectedRows.hasSelections() || reservedRanges.empty()) {
    return selected;
  }

  const auto selectedCount = selectedRows.countSelected();
  selected.reserve(selectedCount);
  if (rows != nullptr) {
    rows->reserve(rows->size() + selectedCount);
  }

  size_t reservedRangeIndex = 0;
  vector_size_t reservedOffset = 0;
  selectedRows.applyToSelected([&](vector_size_t source) {
    while (reservedRangeIndex < reservedRanges.size() &&
           reservedOffset == reservedRanges[reservedRangeIndex].rowCount) {
      ++reservedRangeIndex;
      reservedOffset = 0;
    }
    BOLT_DCHECK_LT(reservedRangeIndex, reservedRanges.size());
    const auto& reserved = reservedRanges[reservedRangeIndex];
    auto* row = reserved.rowBegin + reservedOffset * rowStride;
    selected.push_back(BatchAppendRange{reserved.chunk, row, source, 1});
    if (rows != nullptr) {
      rows->push_back(row);
    }
    ++reservedOffset;
  });

  BOLT_DCHECK_EQ(selected.size(), selectedCount);
  return selected;
}

} // namespace bytedance::bolt::exec::bm
