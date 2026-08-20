/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/BmHashTable.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::exec::test {

class BmHashTableTest : public testing::Test,
                        public bytedance::bolt::test::VectorTestBase {
 protected:
  using BmTable = BmHashTable<true>;

  struct ProbeResults {
    std::vector<vector_size_t> selectedRows;
    std::vector<bool> hasFirstHit;
    std::map<vector_size_t, int32_t> hitCounts;
    std::vector<std::string> resultRows;
  };

  static uint64_t constantCollisionHash(uint64_t /*hash*/) {
    return 0;
  }

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    root_ = memory::memoryManager()->addRootPool(
        testing::UnitTest::GetInstance()->current_test_info()->name(),
        64 << 20,
        memory::MemoryReclaimer::create());

    memory::bm::BufferManagerConfig config;
    config.poolName = root_->name();
    config.spillStoreConfig.fileAllocatorConfig =
        memory::bm::test::ValidConfigWithDirectory(
            memory::bm::test::UniqueTempDir(root_->name()));
    bufferManager_ =
        memory::bm::BufferManager::Create(*root_, std::move(config));
  }

  std::vector<std::unique_ptr<VectorHasher>> makeHashers(
      const RowTypePtr& type,
      int32_t numKeys) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.reserve(numKeys);
    for (auto i = 0; i < numKeys; ++i) {
      hashers.push_back(VectorHasher::create(type->childAt(i), i));
    }
    return hashers;
  }

  std::unique_ptr<BaseHashTable> makeLegacyTable(
      const RowTypePtr& type,
      int32_t numKeys) {
    std::vector<TypePtr> dependents;
    for (auto i = numKeys; i < type->size(); ++i) {
      dependents.push_back(type->childAt(i));
    }
    return HashTable<true>::createForJoin(
        makeHashers(type, numKeys),
        dependents,
        true,
        false,
        BaseHashTable::HashMode::kHash,
        0,
        pool(),
        false);
  }

  std::unique_ptr<BmTable> makeBmTable(
      const RowTypePtr& type,
      int32_t numKeys) {
    std::vector<TypePtr> dependents;
    for (auto i = numKeys; i < type->size(); ++i) {
      dependents.push_back(type->childAt(i));
    }
    return BmTable::createForJoin(
        makeHashers(type, numKeys),
        dependents,
        true,
        false,
        0,
        pool(),
        bufferManager_,
        false);
  }

  void appendBuildRows(
      BaseHashTable& table,
      const RowVectorPtr& build,
      int32_t numKeys) {
    SelectivityVector rows(build->size());
    std::vector<DecodedVector> decodedVectors;
    decodedVectors.reserve(build->childrenSize());
    for (const auto& child : build->children()) {
      decodedVectors.emplace_back(*child, rows);
    }

    std::vector<const DecodedVector*> keyDecoders;
    std::vector<const DecodedVector*> dependentDecoders;
    keyDecoders.reserve(numKeys);
    dependentDecoders.reserve(build->childrenSize() - numKeys);
    for (auto i = 0; i < decodedVectors.size(); ++i) {
      if (i < numKeys) {
        keyDecoders.push_back(&decodedVectors[i]);
      } else {
        dependentDecoders.push_back(&decodedVectors[i]);
      }
    }

    table.appendJoinRows(
        rows,
        folly::Range<const DecodedVector* const*>(
            keyDecoders.data(), keyDecoders.size()),
        folly::Range<const DecodedVector* const*>(
            dependentDecoders.data(), dependentDecoders.size()));
    table.prepareJoinTable({});
  }

  std::vector<std::string> serializeHits(
      BaseHashTable& table,
      const RowTypePtr& buildType,
      folly::Range<vector_size_t*> inputRows,
      folly::Range<char**> hits,
      int32_t numRows) {
    std::vector<const char*> constHits(numRows);
    for (auto i = 0; i < numRows; ++i) {
      constHits[i] = hits[i];
    }

    std::vector<VectorPtr> columns;
    columns.reserve(buildType->size());
    for (auto column = 0; column < buildType->size(); ++column) {
      auto result =
          BaseVector::create(buildType->childAt(column), numRows, pool());
      table.extractJoinColumn(constHits.data(), numRows, column, result);
      columns.push_back(result);
    }

    std::vector<std::string> rows;
    rows.reserve(numRows);
    for (auto row = 0; row < numRows; ++row) {
      std::string serialized = std::to_string(inputRows[row]);
      for (const auto& column : columns) {
        serialized.append("|");
        serialized.append(column->toString(row));
      }
      rows.push_back(std::move(serialized));
    }
    return rows;
  }

  ProbeResults probeAndCollect(
      BaseHashTable& table,
      const RowVectorPtr& build,
      const RowVectorPtr& probe,
      int32_t listBatchSize = 17) {
    HashLookup lookup(table.hashers(), false);
    SelectivityVector probeRows(probe->size());
    table.prepareForJoinProbe(lookup, probe, probeRows, true);
    table.joinProbe(lookup);

    ProbeResults results;
    for (auto row : lookup.rows) {
      results.selectedRows.push_back(row);
      const bool hasHit = lookup.hits[row] != nullptr;
      results.hasFirstHit.push_back(hasHit);
      results.hitCounts[row] = 0;
    }

    BaseHashTable::JoinResultIterator iter;
    iter.reset(lookup);
    std::vector<vector_size_t> inputRows(listBatchSize);
    std::vector<char*> hits(listBatchSize);
    const auto buildType = std::dynamic_pointer_cast<const RowType>(build->type());
    if (!buildType) {
      ADD_FAILURE() << "Expected build row type";
      return results;
    }
    while (!iter.atEnd()) {
      const auto numRows = table.listJoinResults(
          iter,
          false,
          folly::Range<vector_size_t*>(inputRows.data(), inputRows.size()),
          folly::Range<char**>(hits.data(), hits.size()));
      if (numRows <= 0) {
        ADD_FAILURE() << "Expected positive join result batch size";
        return results;
      }
      auto serialized = serializeHits(
          table,
          buildType,
          folly::Range<vector_size_t*>(inputRows.data(), numRows),
          folly::Range<char**>(hits.data(), numRows),
          numRows);
      results.resultRows.insert(
          results.resultRows.end(), serialized.begin(), serialized.end());
      for (auto i = 0; i < numRows; ++i) {
        ++results.hitCounts[inputRows[i]];
      }
    }

    std::sort(results.resultRows.begin(), results.resultRows.end());
    return results;
  }

  void expectParity(
      const RowVectorPtr& build,
      const RowVectorPtr& probe,
      int32_t numKeys,
      uint64_t (*bmHashOverride)(uint64_t) = nullptr) {
    const auto buildType = std::dynamic_pointer_cast<const RowType>(build->type());
    ASSERT_NE(buildType, nullptr);

    auto legacy = makeLegacyTable(buildType, numKeys);
    auto bm = makeBmTable(buildType, numKeys);
    bm->testingSetHashOverride(bmHashOverride);
    appendBuildRows(*legacy, build, numKeys);
    appendBuildRows(*bm, build, numKeys);

    EXPECT_EQ(legacy->joinRowCount(), build->size());
    EXPECT_EQ(bm->joinRowCount(), build->size());

    const auto legacyResults = probeAndCollect(*legacy, build, probe);
    const auto bmResults = probeAndCollect(*bm, build, probe);

    EXPECT_EQ(legacyResults.selectedRows, bmResults.selectedRows);
    EXPECT_EQ(legacyResults.hasFirstHit, bmResults.hasFirstHit);
    EXPECT_EQ(legacyResults.hitCounts, bmResults.hitCounts);
    EXPECT_EQ(legacyResults.resultRows, bmResults.resultRows);
  }

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
};

TEST_F(BmHashTableTest, GenericHashFindsEverySupportedScalarType) {
  const auto build = makeRowVector({
      makeFlatVector<int8_t>({1, 2, 3, 4}),
      makeFlatVector<int16_t>({11, 12, 13, 14}),
      makeFlatVector<int32_t>({21, 22, 23, 24}),
      makeFlatVector<int64_t>({31, 32, 33, 34}),
      makeFlatVector<float>({1.5, 2.5, 3.5, 4.5}),
      makeFlatVector<double>({5.5, 6.5, 7.5, 8.5}),
      makeFlatVector<bool>({true, false, true, false}),
      makeFlatVector<Timestamp>({
          Timestamp(1, 10),
          Timestamp(2, 20),
          Timestamp(3, 30),
          Timestamp(4, 40)}),
      makeFlatVector<int128_t>({11, 22, 33, 44}),
      makeFlatVector<std::string>({"a", "bb", "ccc", "dddd"}),
      makeFlatVector<int64_t>({100, 200, 300, 400}),
  });
  const auto probe = makeRowVector({
      makeFlatVector<int8_t>({1, 4, 9}),
      makeFlatVector<int16_t>({11, 14, 19}),
      makeFlatVector<int32_t>({21, 24, 29}),
      makeFlatVector<int64_t>({31, 34, 39}),
      makeFlatVector<float>({1.5, 4.5, 9.5}),
      makeFlatVector<double>({5.5, 8.5, 9.5}),
      makeFlatVector<bool>({true, false, true}),
      makeFlatVector<Timestamp>({
          Timestamp(1, 10),
          Timestamp(4, 40),
          Timestamp(9, 90)}),
      makeFlatVector<int128_t>({11, 44, 99}),
      makeFlatVector<std::string>({"a", "dddd", "miss"}),
  });
  expectParity(build, probe, 10);
}

TEST_F(BmHashTableTest, HashCollisionStillChecksKeys) {
  const auto build = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4}),
      makeFlatVector<std::string>({"aa", "bb", "cc", "dd"}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });
  const auto probe = makeRowVector({
      makeFlatVector<int64_t>({1, 3, 3, 5}),
      makeFlatVector<std::string>({"aa", "cc", "xx", "miss"}),
  });
  expectParity(build, probe, 2, &BmHashTableTest::constantCollisionHash);
}

TEST_F(BmHashTableTest, DuplicateKeysSpanOutputBatches) {
  std::vector<int64_t> keys(3000, 7);
  std::vector<int64_t> payload(3000);
  std::iota(payload.begin(), payload.end(), 0);
  const auto build = makeRowVector({
      makeFlatVector<int64_t>(keys),
      makeFlatVector<int64_t>(payload),
  });
  const auto probe = makeRowVector({
      makeFlatVector<int64_t>({7, 8}),
  });
  const auto buildType = std::dynamic_pointer_cast<const RowType>(build->type());
  ASSERT_NE(buildType, nullptr);
  auto legacy = makeLegacyTable(buildType, 1);
  auto bm = makeBmTable(buildType, 1);
  appendBuildRows(*legacy, build, 1);
  appendBuildRows(*bm, build, 1);

  EXPECT_EQ(legacy->joinRowCount(), 3000);
  EXPECT_EQ(bm->joinRowCount(), 3000);
  EXPECT_EQ(bm->numDistinct(), 1);
  EXPECT_EQ(bm->stats().numDistinct, 1);

  const auto legacyResults = probeAndCollect(*legacy, build, probe);
  const auto bmResults = probeAndCollect(*bm, build, probe);
  EXPECT_FLOAT_EQ(bm->getDistinctRatio(), 0.5);
  EXPECT_EQ(legacyResults.selectedRows, bmResults.selectedRows);
  EXPECT_EQ(legacyResults.hasFirstHit, bmResults.hasFirstHit);
  EXPECT_EQ(legacyResults.hitCounts, bmResults.hitCounts);
  EXPECT_EQ(legacyResults.resultRows, bmResults.resultRows);
}

TEST_F(BmHashTableTest, CompositeKeyAndNullsMatchLegacy) {
  const auto build = makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt, 1, 2, std::nullopt}),
      makeNullableFlatVector<std::string>(
          {"x", "y", std::nullopt, "z", std::nullopt}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
  });
  const auto probe = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 1, 2, std::nullopt, 3}),
      makeNullableFlatVector<std::string>(
          {"x", std::nullopt, "z", "y", "missing"}),
  });
  expectParity(build, probe, 2);
}

TEST_F(BmHashTableTest, RehashPreservesHits) {
  const auto size = 4096;
  const auto build = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          size, [](auto row) { return std::to_string(row); }),
      makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
  });
  const auto probe = makeRowVector({
      makeFlatVector<int64_t>({0, 1024, 2048, 3072, 4097}),
      makeFlatVector<std::string>(
          {"0", "1024", "2048", "3072", "miss"}),
  });

  const auto buildType = std::dynamic_pointer_cast<const RowType>(build->type());
  ASSERT_NE(buildType, nullptr);
  auto legacy = makeLegacyTable(buildType, 2);
  auto bm = makeBmTable(buildType, 2);
  appendBuildRows(*legacy, build, 2);
  appendBuildRows(*bm, build, 2);

  EXPECT_GT(bm->capacity(), 2048);
  EXPECT_EQ(bm->joinRowCount(), size);
  EXPECT_EQ(bm->numDistinct(), size);
  EXPECT_EQ(bm->stats().numDistinct, size);
  EXPECT_GT(bm->stats().numRehashes, 0);
  EXPECT_EQ(bm->stats().numRehashes, 2);

  const auto legacyResults = probeAndCollect(*legacy, build, probe);
  const auto bmResults = probeAndCollect(*bm, build, probe);
  EXPECT_EQ(legacyResults.selectedRows, bmResults.selectedRows);
  EXPECT_EQ(legacyResults.hasFirstHit, bmResults.hasFirstHit);
  EXPECT_EQ(legacyResults.hitCounts, bmResults.hitCounts);
  EXPECT_EQ(legacyResults.resultRows, bmResults.resultRows);
}

} // namespace bytedance::bolt::exec::test
