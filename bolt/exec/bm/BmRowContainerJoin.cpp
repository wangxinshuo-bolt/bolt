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
  bool hasLastReservedRangeIndex = false;
  size_t lastReservedRangeIndex = 0;
  auto advanceReserved = [&]() {
    while (reservedRangeIndex < reservedRanges.size() &&
           reservedOffset == reservedRanges[reservedRangeIndex].rowCount) {
      ++reservedRangeIndex;
      reservedOffset = 0;
    }
  };

  selectedRows.applyToSelected([&](vector_size_t source) {
    advanceReserved();
    BOLT_DCHECK_LT(reservedRangeIndex, reservedRanges.size());
    const auto rangeIndex = reservedRangeIndex;
    const auto rangeOffset = reservedOffset;
    auto* row = reservedRanges[rangeIndex].rowBegin + rangeOffset * rowStride;
    if (rows != nullptr) {
      rows->push_back(row);
    }
    ++reservedOffset;

    if (!selected.empty()) {
      auto& last = selected.back();
      const auto* expectedRow = last.rowBegin + last.rowCount * rowStride;
      if (hasLastReservedRangeIndex && lastReservedRangeIndex == rangeIndex &&
          last.chunk == reservedRanges[rangeIndex].chunk &&
          last.sourceBegin + last.rowCount == source && expectedRow == row) {
        ++last.rowCount;
        return;
      }
    }

    selected.push_back(
        BatchAppendRange{reservedRanges[rangeIndex].chunk, row, source, 1});
    lastReservedRangeIndex = rangeIndex;
    hasLastReservedRangeIndex = true;
  });

  vector_size_t totalRowCount = 0;
  for (const auto& range : selected) {
    totalRowCount += range.rowCount;
  }
  BOLT_DCHECK_EQ(totalRowCount, selectedCount);
  return selected;
}

} // namespace bytedance::bolt::exec::bm
