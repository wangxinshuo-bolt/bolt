#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/type/HugeInt.h"

#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(
    BmRowContainerTest,
    JoinLayoutKeepsUserCellsAndRuntimeMetadataDisjoint) {
  BmJoinLayoutOptions joinOptions{
      .numKeys = 1,
      .hasNext = true,
      .hasProbedFlag = true,
      .hasNormalizedKey = true,
  };
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      joinOptions);

  auto input = makeRowVector({
      makeFlatVector<int64_t>({101, 202}),
      makeFlatVector<std::string>({"left-row", "right-row"}),
  });
  auto rows = storeAll(container, input);
  ASSERT_EQ(2, rows.size());

  container.setNext(rows[0], rows[1]);
  container.setProbed(rows[0], true);
  container.setNormalizedKey(rows[0], 0x0102030405060708ULL);
  container.setNext(rows[1], rows[0]);
  container.setProbed(rows[1], false);
  container.setNormalizedKey(rows[1], 0x8877665544332211ULL);

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_EQ(101, bigintFlat->valueAt(0));
  EXPECT_EQ(202, bigintFlat->valueAt(1));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("left-row", varcharFlat->valueAt(0).str());
  EXPECT_EQ("right-row", varcharFlat->valueAt(1).str());

  EXPECT_EQ(rows[1], container.next(rows[0]));
  EXPECT_TRUE(container.probed(rows[0]));
  EXPECT_EQ(0x0102030405060708ULL, container.normalizedKey(rows[0]));
  EXPECT_EQ(rows[0], container.next(rows[1]));
  EXPECT_FALSE(container.probed(rows[1]));
  EXPECT_EQ(0x8877665544332211ULL, container.normalizedKey(rows[1]));
  EXPECT_GT(container.rowSize(), sizeof(int64_t) + sizeof(StringView));
}

TEST_F(BmRowContainerTest, JoinMetadataRoundTripsAndResets) {
  BmJoinLayoutOptions joinOptions{
      .numKeys = 1,
      .hasNext = true,
      .hasProbedFlag = true,
      .hasNormalizedKey = true,
  };
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      joinOptions);

  auto input = makeRowVector({
      makeFlatVector<int64_t>({9, 13}),
      makeFlatVector<std::string>({"before-reset", "other-row"}),
  });
  auto rows = storeAll(container, input);
  ASSERT_EQ(2, rows.size());

  container.setNext(rows[0], rows[1]);
  container.setProbed(rows[0], true);
  container.setNormalizedKey(rows[0], 97);
  container.setNext(rows[1], rows[0]);
  container.setProbed(rows[1], true);
  container.setNormalizedKey(rows[1], 31337);

  container.resetJoinRuntimeMetadata(rows[0]);

  EXPECT_EQ(nullptr, container.next(rows[0]));
  EXPECT_FALSE(container.probed(rows[0]));
  EXPECT_EQ(0, container.normalizedKey(rows[0]));

  EXPECT_EQ(rows[0], container.next(rows[1]));
  EXPECT_TRUE(container.probed(rows[1]));
  EXPECT_EQ(31337, container.normalizedKey(rows[1]));

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_EQ(9, bigintFlat->valueAt(0));
  EXPECT_EQ(13, bigintFlat->valueAt(1));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("before-reset", varcharFlat->valueAt(0).str());
  EXPECT_EQ("other-row", varcharFlat->valueAt(1).str());
}

TEST_F(BmRowContainerTest, JoinMetadataDefaultsAreZero) {
  BmJoinLayoutOptions joinOptions{
      .numKeys = 1,
      .hasNext = true,
      .hasProbedFlag = true,
      .hasNormalizedKey = true,
  };
  BmRowContainer container(
      {BIGINT()},
      {false},
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      joinOptions);

  auto context = container.appendRow();
  auto* row = context.row();
  *reinterpret_cast<int64_t*>(row) = 77;

  EXPECT_EQ(nullptr, container.next(row));
  EXPECT_FALSE(container.probed(row));
  EXPECT_EQ(0, container.normalizedKey(row));

  container.setNext(row, row);
  container.setProbed(row, true);
  container.setNormalizedKey(row, 1234);
  container.resetJoinRuntimeMetadata(row);

  EXPECT_EQ(nullptr, container.next(row));
  EXPECT_FALSE(container.probed(row));
  EXPECT_EQ(0, container.normalizedKey(row));
  EXPECT_EQ(77, *reinterpret_cast<const int64_t*>(row));
}

TEST_F(BmRowContainerTest, AppendBatchSelectedPreservesSelectionOrder) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      makeFlatVector<std::string>({"ten", "twenty", "thirty", "forty", "fifty"}),
  });
  SelectivityVector selected(input->size(), false);
  selected.setValid(1, true);
  selected.setValid(3, true);
  selected.setValid(4, true);
  selected.updateBounds();

  auto nextBefore = container.activeSegmentNextRowNumber();
  std::vector<char*> rows;
  container.appendBatchSelected(input, selected, kDefaultPartition, &rows);

  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(nextBefore + 3, container.activeSegmentNextRowNumber());
  EXPECT_EQ(3, container.numRows());

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_EQ(20, bigintFlat->valueAt(0));
  EXPECT_EQ(40, bigintFlat->valueAt(1));
  EXPECT_EQ(50, bigintFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("twenty", varcharFlat->valueAt(0).str());
  EXPECT_EQ("forty", varcharFlat->valueAt(1).str());
  EXPECT_EQ("fifty", varcharFlat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, AppendBatchSelectedPreservesNullsAndLongDecimal) {
  auto longDecimalType = DECIMAL(30, 4);
  BmRowContainer container(
      {BIGINT(), longDecimalType, VARCHAR()},
      {true, true, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>(
          {111, std::nullopt, 333, 444, std::nullopt}),
      makeNullableFlatVector<int128_t>(
          {HugeInt::build(1, 11),
           std::nullopt,
           HugeInt::build(3, 33),
           HugeInt::build(4, 44),
           HugeInt::build(5, 55)},
          longDecimalType),
      makeNullableFlatVector<std::string>(
          {"keep-0",
           std::nullopt,
           std::string(40, 'x'),
           "skip-3",
           std::string(48, 'z')}),
  });
  SelectivityVector selected(input->size(), false);
  selected.setValid(0, true);
  selected.setValid(2, true);
  selected.setValid(4, true);
  selected.updateBounds();

  auto nextBefore = container.activeSegmentNextRowNumber();
  std::vector<char*> rows;
  container.appendBatchSelected(input, selected, kDefaultPartition, &rows);

  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(nextBefore + 3, container.activeSegmentNextRowNumber());
  EXPECT_EQ(3, container.numRows());

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_FALSE(bigintFlat->isNullAt(0));
  EXPECT_FALSE(bigintFlat->isNullAt(1));
  EXPECT_TRUE(bigintFlat->isNullAt(2));
  EXPECT_EQ(111, bigintFlat->valueAt(0));
  EXPECT_EQ(333, bigintFlat->valueAt(1));

  auto decimalResult = BaseVector::create(longDecimalType, rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, decimalResult);
  auto decimalFlat = decimalResult->asFlatVector<int128_t>();
  ASSERT_NE(nullptr, decimalFlat);
  EXPECT_FALSE(decimalFlat->isNullAt(0));
  EXPECT_FALSE(decimalFlat->isNullAt(1));
  EXPECT_FALSE(decimalFlat->isNullAt(2));
  EXPECT_EQ(HugeInt::build(1, 11), decimalFlat->valueAt(0));
  EXPECT_EQ(HugeInt::build(3, 33), decimalFlat->valueAt(1));
  EXPECT_EQ(HugeInt::build(5, 55), decimalFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 2, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_FALSE(varcharFlat->isNullAt(0));
  EXPECT_FALSE(varcharFlat->isNullAt(1));
  EXPECT_FALSE(varcharFlat->isNullAt(2));
  EXPECT_EQ("keep-0", varcharFlat->valueAt(0).str());
  EXPECT_EQ(std::string(40, 'x'), varcharFlat->valueAt(1).str());
  EXPECT_EQ(std::string(48, 'z'), varcharFlat->valueAt(2).str());
}

} // namespace
} // namespace bytedance::bolt::exec::bm
