#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/BoltException.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"

#include <cmath>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {
struct BmRowContainerTestPeer {
  static std::vector<BatchAppendRange> selectedRanges(
      const std::vector<BatchAppendRange>& reservedRanges,
      const SelectivityVector& selectedRows,
      std::vector<char*>* rows,
      uint32_t rowStride) {
    return BmRowContainer::selectedRanges(
        reservedRanges, selectedRows, rows, rowStride);
  }

  static uint64_t partitionGeneration(
      const BmRowContainer& container,
      PartitionId partition) {
    return container.partitionGenerations_[partition];
  }

  static uint32_t partitionLeaseCount(
      const BmRowContainer& container,
      PartitionId partition) {
    return container.partitionLeaseCounts_[partition];
  }

  static uint64_t segmentGeneration(
      const BmRowContainer& container,
      SegmentId segment) {
    return container.segments_.segmentData(segment).meta.generation;
  }

  static bool rowBlockHandleValid(
      const BmRowContainer& container,
      SegmentId segment,
      ChunkId chunk) {
    return container.segments_.segmentData(segment).chunks[chunk]->rowBlock.handle
        .valid();
  }

  static bool rowBlockDirty(
      const BmRowContainer& container,
      SegmentId segment,
      ChunkId chunk) {
    return container.bufferManager_->IsDirty(
        container.segments_.segmentData(segment).chunks[chunk]->rowBlock.block);
  }

  static void setLeaseGeneration(BmRoundLease& lease, uint64_t generation) {
    lease.generation_ = generation;
  }
};

namespace {

using bytedance::bolt::exec::VectorHasher;
using bytedance::bolt::memory::bm::MemoryTag;

std::vector<char*> storeAllLegacy(
    exec::RowContainer& container,
    const RowVectorPtr& input) {
  SelectivityVector rows(input->size());
  std::vector<DecodedVector> decoded(input->childrenSize());
  for (auto i = 0; i < input->childrenSize(); ++i) {
    decoded[i].decode(*input->childAt(i), rows);
  }

  std::vector<char*> out(input->size());
  for (auto row = 0; row < input->size(); ++row) {
    out[row] = container.newRow();
    for (auto column = 0; column < input->childrenSize(); ++column) {
      container.store(decoded[column], row, out[row], column);
    }
  }
  return out;
}

std::vector<uint64_t> vectorHasherExpected(
    const RowVectorPtr& input,
    folly::Range<const int32_t*> keyColumns) {
  SelectivityVector selected(input->size());
  raw_vector<uint64_t> hashes(input->size());
  for (auto i = 0; i < keyColumns.size(); ++i) {
    auto column = keyColumns[i];
    auto hasher = VectorHasher::create(input->childAt(column)->type(), column);
    hasher->decode(*input->childAt(column), selected);
    hasher->hash(selected, i > 0, hashes);
  }
  return std::vector<uint64_t>(hashes.begin(), hashes.end());
}

std::vector<uint64_t> toStdVector(const raw_vector<uint64_t>& hashes) {
  return std::vector<uint64_t>(hashes.begin(), hashes.end());
}

std::vector<TypePtr> childTypes(const RowVectorPtr& input) {
  std::vector<TypePtr> types;
  types.reserve(input->childrenSize());
  for (auto i = 0; i < input->childrenSize(); ++i) {
    types.push_back(input->childAt(i)->type());
  }
  return types;
}

DecodedVector decodeAll(const BaseVector& vector) {
  DecodedVector decoded;
  SelectivityVector rows(vector.size());
  decoded.decode(vector, rows);
  return decoded;
}

std::vector<uint64_t> legacyHashes(
    exec::RowContainer& container,
    std::vector<char*>& rows,
    folly::Range<const int32_t*> keyColumns) {
  std::vector<uint64_t> hashes(rows.size(), 0);
  for (auto keyIndex = 0; keyIndex < keyColumns.size(); ++keyIndex) {
    container.hash(
        keyColumns[keyIndex],
        {rows.data(), rows.size()},
        keyIndex > 0,
        hashes.data());
  }
  return hashes;
}

void expectRowsMatchDecoded(
    BmRowContainer& container,
    const std::vector<char*>& rows,
    const RowVectorPtr& input,
    bool nullsEqual = true) {
  for (auto column = 0; column < input->childrenSize(); ++column) {
    auto decoded = decodeAll(*input->childAt(column));
    for (auto row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(
          container.equalsDecoded(rows[row], column, decoded, row, nullsEqual))
          << "column " << column << " row " << row;
    }
  }
}

void expectRowsRemainReadableAndEqual(
    BmRowContainer& container,
    const std::vector<char*>& rows,
    const RowVectorPtr& input) {
  ASSERT_EQ(rows.size(), input->size());
  expectRowsMatchDecoded(container, rows, input);
}

void expectRowsRemainReadableAndEqual(
    BmRowContainer& container,
    const std::vector<const char*>& rows,
    const RowVectorPtr& input) {
  ASSERT_EQ(rows.size(), input->size());
  for (auto column = 0; column < input->childrenSize(); ++column) {
    auto decoded = decodeAll(*input->childAt(column));
    for (auto row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(container.equalsDecoded(rows[row], column, decoded, row, true))
          << "column " << column << " row " << row;
    }
  }
}

TEST_F(BmRowContainerTest, EqualsDecodedMatchesBaseVectorForJoinKeys) {
  const auto shortDecimal = DECIMAL(12, 3);
  const auto longDecimal = DECIMAL(30, 4);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({11, 22, std::nullopt, 44}),
      makeNullableFlatVector<double>({1.25, -0.5, 3.5, std::nullopt}),
      makeNullableFlatVector<int32_t>(
          {DATE()->toDays("2024-01-01"),
           DATE()->toDays("2024-01-02"),
           std::nullopt,
           DATE()->toDays("2024-01-04")},
          DATE()),
      makeNullableFlatVector<Timestamp>(
          {Timestamp(100, 7),
           Timestamp(200, 8),
           Timestamp(300, 9),
           std::nullopt},
          TIMESTAMP()),
      makeNullableFlatVector<int64_t>(
          {12345, std::nullopt, -33333, 44444}, shortDecimal),
      makeNullableFlatVector<int128_t>(
          {HugeInt::build(1, 11),
           HugeInt::build(2, 22),
           std::nullopt,
           HugeInt::build(4, 44)},
          longDecimal),
      makeNullableFlatVector<std::string>(
          {"alpha", std::nullopt, std::string(40, 'x'), "delta"}),
      makeNullableFlatVector<std::string>(
          {"\x01\x02", "\x03\x04\x05", std::nullopt, std::string(36, '\x7f')},
          VARBINARY()),
  });
  BmRowContainer container(
      childTypes(input),
      std::vector<bool>(input->childrenSize(), true),
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      BmJoinLayoutOptions{.numKeys = 8});
  auto rows = storeAll(container, input);

  SelectivityVector selected(input->size());
  for (auto column = 0; column < input->childrenSize(); ++column) {
    DecodedVector decoded;
    decoded.decode(*input->childAt(column), selected);
    for (auto row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(container.equalsDecoded(rows[row], column, decoded, row, true))
          << "column " << column << " row " << row;
      EXPECT_EQ(
          !input->childAt(column)->isNullAt(row),
          container.equalsDecoded(rows[row], column, decoded, row, false))
          << "nullsEqual=false should only reject the row's own null value at column "
          << column << " row " << row;
      const auto other = (row + 1) % input->size();
      const auto expected = input->childAt(column)->equalValueAt(
          input->childAt(column).get(), row, other);
      EXPECT_EQ(
          expected,
          container.equalsDecoded(rows[row], column, decoded, other, true))
          << "column " << column << " row " << row << " other " << other;
    }
  }
}

TEST_F(BmRowContainerTest, HashRowsMatchesVectorHasherForScalarAndCompositeKeys) {
  auto input = makeRowVector({
      makeNullableFlatVector<int32_t>({10, std::nullopt, 30, 10}),
      makeNullableFlatVector<std::string>(
          {"alpha", std::string(40, 'x'), std::nullopt, "alpha"}),
  });
  BmRowContainer container(
      childTypes(input),
      {true, true},
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      BmJoinLayoutOptions{.numKeys = 2});
  auto rows = storeAll(container, input);
  raw_vector<uint64_t> hashes(rows.size());

  std::vector<int32_t> integerKey{0};
  container.hashRows({rows.data(), rows.size()}, {integerKey.data(), 1}, hashes);
  EXPECT_EQ(vectorHasherExpected(input, {integerKey.data(), 1}), toStdVector(hashes));

  std::fill(hashes.begin(), hashes.end(), 0);
  std::vector<int32_t> varcharKey{1};
  container.hashRows({rows.data(), rows.size()}, {varcharKey.data(), 1}, hashes);
  EXPECT_EQ(vectorHasherExpected(input, {varcharKey.data(), 1}), toStdVector(hashes));

  std::fill(hashes.begin(), hashes.end(), 0);
  std::vector<int32_t> compositeKey{0, 1};
  container.hashRows(
      {rows.data(), rows.size()}, {compositeKey.data(), 2}, hashes);
  EXPECT_EQ(vectorHasherExpected(input, {compositeKey.data(), 2}), toStdVector(hashes));
}

TEST_F(BmRowContainerTest, HashRowsMatchesVectorHasherForAllSupportedKeyTypes) {
  const auto shortDecimal = DECIMAL(12, 3);
  const auto longDecimal = DECIMAL(30, 4);
  auto input = makeRowVector({
      makeNullableFlatVector<bool>({true, false, std::nullopt, true}),
      makeNullableFlatVector<int8_t>({1, -2, std::nullopt, 4}),
      makeNullableFlatVector<int16_t>({11, std::nullopt, -33, 44}),
      makeNullableFlatVector<int32_t>({101, 202, std::nullopt, -404}),
      makeNullableFlatVector<int64_t>({1001, std::nullopt, -3003, 4004}),
      makeNullableFlatVector<float>({1.25f, -0.5f, std::nullopt, 4.5f}),
      makeNullableFlatVector<double>({2.25, std::nullopt, -3.5, 4.75}),
      makeNullableFlatVector<int32_t>(
          {DATE()->toDays("2024-02-01"),
           std::nullopt,
           DATE()->toDays("2024-02-03"),
           DATE()->toDays("2024-02-04")},
          DATE()),
      makeNullableFlatVector<Timestamp>(
          {Timestamp(10, 11),
           Timestamp(20, 22),
           std::nullopt,
           Timestamp(40, 44)},
          TIMESTAMP()),
      makeNullableFlatVector<int64_t>(
          {12345, std::nullopt, -33333, 44444}, shortDecimal),
      makeNullableFlatVector<int128_t>(
          {HugeInt::build(1, 111),
           HugeInt::build(2, 222),
           std::nullopt,
           HugeInt::build(4, 444)},
          longDecimal),
      makeNullableFlatVector<std::string>(
          {"hash-alpha", std::nullopt, std::string(48, 'v'), "hash-delta"}),
      makeNullableFlatVector<std::string>(
          {"\x00\x01", std::string(40, '\x02'), std::nullopt, "\x03\x04"},
          VARBINARY()),
  });
  BmRowContainer container(
      childTypes(input),
      std::vector<bool>(input->childrenSize(), true),
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      BmJoinLayoutOptions{.numKeys = static_cast<uint32_t>(input->childrenSize())});
  auto rows = storeAll(container, input);
  raw_vector<uint64_t> hashes(rows.size());

  for (int32_t column = 0; column < input->childrenSize(); ++column) {
    std::fill(hashes.begin(), hashes.end(), 0);
    std::vector<int32_t> key{column};
    container.hashRows({rows.data(), rows.size()}, {key.data(), 1}, hashes);
    EXPECT_EQ(vectorHasherExpected(input, {key.data(), 1}), toStdVector(hashes))
        << "column " << column << " type "
        << input->childAt(column)->type()->toString();
  }

  std::fill(hashes.begin(), hashes.end(), 0);
  std::vector<int32_t> compositeKey{3, 11, 12};
  container.hashRows(
      {rows.data(), rows.size()}, {compositeKey.data(), compositeKey.size()}, hashes);
  EXPECT_EQ(
      vectorHasherExpected(input, {compositeKey.data(), compositeKey.size()}),
      toStdVector(hashes));
}

TEST_F(BmRowContainerTest, EqualsDecodedAndHashRowsMatchLegacyRowContainer) {
  auto input = makeRowVector({
      makeNullableFlatVector<float>(
          {-0.0f, 0.0f, std::nullopt, 7.5f, -3.25f}),
      makeNullableFlatVector<double>(
          {-0.0, 0.0, std::nullopt, 11.25, -9.75}),
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3, 1, 5}),
      makeNullableFlatVector<std::string>(
          {"alpha", std::nullopt, "alpha", std::string(48, 'q'), ""}),
  });
  BmRowContainer bmContainer(
      childTypes(input),
      std::vector<bool>(input->childrenSize(), true),
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      BmJoinLayoutOptions{.numKeys = static_cast<uint32_t>(input->childrenSize())});
  auto bmRows = storeAll(bmContainer, input);

  exec::RowContainer legacyContainer(
      childTypes(input),
      true,
      std::vector<exec::Accumulator>{},
      {},
      false,
      true,
      false,
      false,
      false,
      pool());
  auto legacyRows = storeAllLegacy(legacyContainer, input);

  for (auto column = 0; column < input->childrenSize(); ++column) {
    auto decoded = decodeAll(*input->childAt(column));
    auto legacyColumn = legacyContainer.columnAt(column);
    for (auto row = 0; row < input->size(); ++row) {
      const auto expected =
          legacyContainer.compare(legacyRows[row], legacyColumn, decoded, row) == 0;
      EXPECT_EQ(
          expected,
          bmContainer.equalsDecoded(bmRows[row], column, decoded, row, true))
          << "column " << column << " row " << row;
    }
  }

  std::vector<int32_t> compositeKey{0, 1, 2, 3};
  raw_vector<uint64_t> bmHashes(bmRows.size());
  bmContainer.hashRows(
      {bmRows.data(), bmRows.size()}, {compositeKey.data(), compositeKey.size()}, bmHashes);
  EXPECT_EQ(
      legacyHashes(legacyContainer, legacyRows, {compositeKey.data(), compositeKey.size()}),
      toStdVector(bmHashes));
}

TEST_F(BmRowContainerTest, HashRowsMatchesLegacyAndVectorHasherForSignedZeroNullAndCompositeKeys) {
  auto input = makeRowVector({
      makeNullableFlatVector<float>({-0.0f, 0.0f, std::nullopt, -0.0f}),
      makeNullableFlatVector<double>({0.0, -0.0, std::nullopt, -0.0}),
      makeNullableFlatVector<std::string>({"same", "same", std::nullopt, "same"}),
  });
  BmRowContainer bmContainer(
      childTypes(input),
      std::vector<bool>(input->childrenSize(), true),
      bufferManager_,
      MemoryTag::kTesting,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      BmJoinLayoutOptions{.numKeys = 3});
  auto bmRows = storeAll(bmContainer, input);

  exec::RowContainer legacyContainer(
      childTypes(input),
      true,
      std::vector<exec::Accumulator>{},
      {},
      false,
      true,
      false,
      false,
      false,
      pool());
  auto legacyRows = storeAllLegacy(legacyContainer, input);

  for (auto column = 0; column < input->childrenSize(); ++column) {
    auto decoded = decodeAll(*input->childAt(column));
    EXPECT_TRUE(bmContainer.equalsDecoded(bmRows[0], column, decoded, 1, true))
        << "column " << column << " signed zero equality";
  }

  std::vector<int32_t> compositeKey{0, 1, 2};
  raw_vector<uint64_t> bmHashes(bmRows.size());
  bmContainer.hashRows(
      {bmRows.data(), bmRows.size()}, {compositeKey.data(), compositeKey.size()}, bmHashes);
  EXPECT_EQ(
      vectorHasherExpected(input, {compositeKey.data(), compositeKey.size()}),
      toStdVector(bmHashes));
  EXPECT_EQ(
      legacyHashes(legacyContainer, legacyRows, {compositeKey.data(), compositeKey.size()}),
      toStdVector(bmHashes));
}

TEST_F(BmRowContainerTest, SealKeepsPointersResident) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, {false, false}, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<std::string>({"resident", std::string(48, 'r')}),
  });
  auto rows = storeAll(container, input);
  const auto segment = container.sealActivePartitionSegment(kDefaultPartition);

  EXPECT_EQ(SegmentState::kFinalizedResident, container.segmentState(segment));
  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("resident", flat->valueAt(0).str());
  EXPECT_EQ(std::string(48, 'r'), flat->valueAt(1).str());
  EXPECT_TRUE(BmRowContainerTestPeer::rowBlockHandleValid(container, segment, 0));
  EXPECT_EQ(
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition),
      BmRowContainerTestPeer::segmentGeneration(container, segment));

  auto nextBefore = container.activeSegmentNextRowNumber();
  auto appended = container.appendRow();
  EXPECT_NE(nullptr, appended.row());
  EXPECT_EQ(nextBefore + 1, container.activeSegmentNextRowNumber());
  EXPECT_EQ(
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition),
      BmRowContainerTestPeer::segmentGeneration(
          container, container.activeSegmentId(kDefaultPartition)));
}

TEST_F(BmRowContainerTest, SpillAndReloadCreatesFreshEpoch) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      64 << 10);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({5, 6}),
      makeFlatVector<std::string>({std::string(80, 'a'), std::string(80, 'b')}),
  });
  auto rows = storeAll(container, input);
  const auto segment = container.sealActivePartitionSegment(kDefaultPartition);
  auto lease = container.acquireRoundLease(kDefaultPartition);
  const auto generationBeforeRelease =
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition);
  EXPECT_THROW(
      container.spillSealedPartition(kDefaultPartition), BoltRuntimeError);
  EXPECT_TRUE(container.equalsDecoded(
      rows[0], 0, DecodedVector(*input->childAt(0)), 0, true));
  EXPECT_EQ(
      generationBeforeRelease,
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition));
  EXPECT_TRUE(BmRowContainerTestPeer::rowBlockHandleValid(container, segment, 0));

  container.releaseRoundLease(lease);
  EXPECT_THROW(
      container.equalsDecoded(
          rows[0], 0, DecodedVector(*input->childAt(0)), 0, true),
      BoltRuntimeError);
  auto spilled = container.spillSealedPartition(kDefaultPartition);
  EXPECT_EQ(segment, spilled);
  EXPECT_FALSE(BmRowContainerTestPeer::rowBlockHandleValid(container, segment, 0));
  auto reloaded = container.loadPartitionRows(kDefaultPartition);
  ASSERT_EQ(rows.size(), reloaded.size());
  EXPECT_TRUE(BmRowContainerTestPeer::rowBlockHandleValid(container, segment, 0));
  EXPECT_TRUE(BmRowContainerTestPeer::rowBlockDirty(container, segment, 0));
  EXPECT_TRUE(container.equalsDecoded(
      reloaded[0], 1, DecodedVector(*input->childAt(1)), 0, true));
  EXPECT_TRUE(container.equalsDecoded(
      reloaded[1], 1, DecodedVector(*input->childAt(1)), 1, true));
  EXPECT_EQ(
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition),
      BmRowContainerTestPeer::segmentGeneration(container, segment));
}

TEST_F(BmRowContainerTest, SecondSpillAndReloadResetsRuntimeMetadata) {
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
      64 << 10,
      64 << 10,
      joinOptions);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({7, 8}),
      makeFlatVector<std::string>({std::string(96, 'x'), std::string(96, 'y')}),
  });
  auto rows = storeAll(container, input);
  container.setNext(rows[0], rows[1]);
  container.setProbed(rows[0], true);
  container.setNormalizedKey(rows[0], 12345);

  container.sealActivePartitionSegment(kDefaultPartition);
  container.spillSealedPartition(kDefaultPartition);
  auto firstReload = container.loadPartitionRows(kDefaultPartition);
  ASSERT_EQ(2, firstReload.size());
  EXPECT_TRUE(BmRowContainerTestPeer::rowBlockDirty(container, 1, 0));
  container.setNext(firstReload[0], firstReload[1]);
  container.setProbed(firstReload[0], true);
  container.setNormalizedKey(firstReload[0], 67890);

  container.spillSealedPartition(kDefaultPartition);
  EXPECT_THROW(
      container.equalsDecoded(
          firstReload[0], 1, DecodedVector(*input->childAt(1)), 0, true),
      BoltRuntimeError);
  auto secondReload = container.loadPartitionRows(kDefaultPartition);
  ASSERT_EQ(2, secondReload.size());
  EXPECT_EQ(nullptr, container.next(secondReload[0]));
  EXPECT_FALSE(container.probed(secondReload[0]));
  EXPECT_EQ(0, container.normalizedKey(secondReload[0]));
  EXPECT_TRUE(container.equalsDecoded(
      secondReload[0], 1, DecodedVector(*input->childAt(1)), 0, true));
}

TEST_F(BmRowContainerTest, RoundLeaseRejectsEveryReleasePrimitive) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      32);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({4, 1, 3, 2}),
      makeFlatVector<std::string>({"four", "one", "three", "two"}),
  });
  auto rows = storeAll(container, input);
  std::vector<char*> ordered{rows[1], rows[3], rows[2], rows[0]};
  const auto segment =
      container.finalizeReorderedSegment({ordered.data(), ordered.size()});
  auto window = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = window.listRowIds();
  ASSERT_FALSE(rowIds.empty());
  auto loadedRows = window.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(rowIds.size(), loadedRows.size());
  const auto generationBefore =
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition);
  const auto batchPinsBefore = bufferManager_->stats().batchPinCount;

  {
    auto lease = container.acquireRoundLease(kDefaultPartition);
    EXPECT_EQ(
        1,
        BmRowContainerTestPeer::partitionLeaseCount(
            container, kDefaultPartition));
    EXPECT_THROW(container.releaseSegment(segment), BoltRuntimeError);
    EXPECT_THROW(
        container.releaseSegments({&segment, 1}), BoltRuntimeError);
    EXPECT_THROW(container.releaseChunk(segment, 0), BoltRuntimeError);
    EXPECT_THROW(container.popFrontRows(1), BoltRuntimeError);
    EXPECT_THROW(window.releaseLoadedChunks(), BoltRuntimeError);
    EXPECT_THROW(window.evictLoadedChunks(), BoltRuntimeError);
    EXPECT_THROW(
        container.spillSealedPartition(kDefaultPartition), BoltRuntimeError);
    EXPECT_THROW(container.spillActiveSegment(), BoltRuntimeError);
    EXPECT_THROW(
        container.spillActivePartitionSegment(kDefaultPartition),
        BoltRuntimeError);

    auto appendInput = makeRowVector({
        makeFlatVector<int64_t>({9, 8}),
        makeFlatVector<std::string>({"nine", "eight"}),
    });
    EXPECT_THROW((void)container.appendRow(), BoltRuntimeError);
    EXPECT_THROW(container.appendBatch(appendInput), BoltRuntimeError);
    SelectivityVector selected(appendInput->size(), false);
    selected.setValid(0, true);
    selected.updateBounds();
    EXPECT_THROW(
        container.appendBatchSelected(appendInput, selected),
        BoltRuntimeError);

    auto result = BaseVector::create(VARCHAR(), loadedRows.size(), pool());
    container.extractColumnResident(
        loadedRows.data(), loadedRows.size(), 1, result);
    auto flat = result->asFlatVector<StringView>();
    ASSERT_NE(nullptr, flat);
    EXPECT_EQ("one", flat->valueAt(0).str());
    EXPECT_EQ("two", flat->valueAt(1).str());

    auto merge = container.beginMergeReadSegments({&segment, 1});
    std::vector<char*> batch;
    ASSERT_TRUE(merge.next(batch, 1));
    EXPECT_THROW(merge.next(batch, 1), BoltRuntimeError);
    EXPECT_EQ(
        generationBefore,
        BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition));
    EXPECT_TRUE(BmRowContainerTestPeer::rowBlockHandleValid(container, segment, 0));
    EXPECT_EQ(batchPinsBefore, bufferManager_->stats().batchPinCount);
    expectRowsRemainReadableAndEqual(container, loadedRows, makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4}),
        makeFlatVector<std::string>({"one", "two", "three", "four"}),
    }));
    container.releaseRoundLease(lease);
  }
}

TEST_F(BmRowContainerTest, RoundLeaseDestructorAndMoveAssignmentReleaseOwnership) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, {false, false}, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<std::string>({"one", "two"}),
  });
  auto rows = storeAll(container, input);
  const auto generationBefore =
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition);

  {
    auto first = container.acquireRoundLease(kDefaultPartition);
    EXPECT_EQ(
        1,
        BmRowContainerTestPeer::partitionLeaseCount(container, kDefaultPartition));
    BmRoundLease second;
    second = std::move(first);
    EXPECT_FALSE(first.active());
    EXPECT_TRUE(second.active());
    EXPECT_EQ(
        1,
        BmRowContainerTestPeer::partitionLeaseCount(container, kDefaultPartition));
    expectRowsRemainReadableAndEqual(container, rows, input);
  }

  EXPECT_EQ(
      0,
      BmRowContainerTestPeer::partitionLeaseCount(container, kDefaultPartition));
  EXPECT_EQ(
      generationBefore + 1,
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition));
  EXPECT_THROW(
      container.equalsDecoded(rows[0], 0, decodeAll(*input->childAt(0)), 0, true),
      BoltRuntimeError);
}

TEST_F(BmRowContainerTest, RoundLeaseRejectsStaleTokenRelease) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()}, {false, false}, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({3, 4}),
      makeFlatVector<std::string>({"three", "four"}),
  });
  auto rows = storeAll(container, input);
  auto lease = container.acquireRoundLease(kDefaultPartition);
  const auto initialGeneration =
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition);
  BmRowContainerTestPeer::setLeaseGeneration(lease, initialGeneration - 1);

  EXPECT_THROW(container.releaseRoundLease(lease), BoltRuntimeError);
  EXPECT_EQ(
      initialGeneration,
      BmRowContainerTestPeer::partitionGeneration(container, kDefaultPartition));
  EXPECT_EQ(
      1,
      BmRowContainerTestPeer::partitionLeaseCount(container, kDefaultPartition));
  expectRowsRemainReadableAndEqual(container, rows, input);

  BmRowContainerTestPeer::setLeaseGeneration(lease, initialGeneration);
  container.releaseRoundLease(lease);
  EXPECT_EQ(
      0,
      BmRowContainerTestPeer::partitionLeaseCount(container, kDefaultPartition));
}

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

TEST_F(BmRowContainerTest, SelectedRangesCoalescesContiguousRows) {
  ChunkData firstChunk;
  firstChunk.meta.id = 11;
  ChunkData secondChunk;
  secondChunk.meta.id = 12;

  std::vector<char> firstStorage(32);
  std::vector<char> secondStorage(16);
  const uint32_t rowStride = 8;
  std::vector<BatchAppendRange> reservedRanges{
      BatchAppendRange{&firstChunk, firstStorage.data(), 0, 3},
      BatchAppendRange{&secondChunk, secondStorage.data(), 3, 2},
  };

  SelectivityVector selected(8, false);
  selected.setValid(1, true);
  selected.setValid(2, true);
  selected.setValid(3, true);
  selected.setValid(6, true);
  selected.setValid(7, true);
  selected.updateBounds();

  std::vector<char*> rows;
  auto ranges = BmRowContainerTestPeer::selectedRanges(
      reservedRanges, selected, &rows, rowStride);

  ASSERT_EQ(2, ranges.size());
  EXPECT_EQ(
      5,
      std::accumulate(
          ranges.begin(),
          ranges.end(),
          0,
          [](int total, const BatchAppendRange& range) {
            return total + range.rowCount;
          }));
  EXPECT_EQ(&firstChunk, ranges[0].chunk);
  EXPECT_EQ(firstStorage.data(), ranges[0].rowBegin);
  EXPECT_EQ(1, ranges[0].sourceBegin);
  EXPECT_EQ(3, ranges[0].rowCount);

  EXPECT_EQ(&secondChunk, ranges[1].chunk);
  EXPECT_EQ(secondStorage.data(), ranges[1].rowBegin);
  EXPECT_EQ(6, ranges[1].sourceBegin);
  EXPECT_EQ(2, ranges[1].rowCount);

  ASSERT_EQ(5, rows.size());
  EXPECT_EQ(firstStorage.data(), rows[0]);
  EXPECT_EQ(firstStorage.data() + rowStride, rows[1]);
  EXPECT_EQ(firstStorage.data() + 2 * rowStride, rows[2]);
  EXPECT_EQ(secondStorage.data(), rows[3]);
  EXPECT_EQ(secondStorage.data() + rowStride, rows[4]);
}

TEST_F(
    BmRowContainerTest,
    SelectedRangesRespectsReservedRangeBoundariesWithinChunk) {
  ChunkData chunk;
  chunk.meta.id = 21;

  std::vector<char> storage(24);
  const uint32_t rowStride = 8;
  std::vector<BatchAppendRange> reservedRanges{
      BatchAppendRange{&chunk, storage.data(), 0, 2},
      BatchAppendRange{&chunk, storage.data() + 2 * rowStride, 2, 1},
  };

  SelectivityVector selected(5, false);
  selected.setValid(1, true);
  selected.setValid(2, true);
  selected.setValid(3, true);
  selected.updateBounds();

  auto ranges = BmRowContainerTestPeer::selectedRanges(
      reservedRanges, selected, nullptr, rowStride);

  ASSERT_EQ(2, ranges.size());
  EXPECT_EQ(
      3,
      std::accumulate(
          ranges.begin(),
          ranges.end(),
          0,
          [](int total, const BatchAppendRange& range) {
            return total + range.rowCount;
          }));
  EXPECT_EQ(&chunk, ranges[0].chunk);
  EXPECT_EQ(storage.data(), ranges[0].rowBegin);
  EXPECT_EQ(1, ranges[0].sourceBegin);
  EXPECT_EQ(2, ranges[0].rowCount);

  EXPECT_EQ(&chunk, ranges[1].chunk);
  EXPECT_EQ(storage.data() + 2 * rowStride, ranges[1].rowBegin);
  EXPECT_EQ(3, ranges[1].sourceBegin);
  EXPECT_EQ(1, ranges[1].rowCount);
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
