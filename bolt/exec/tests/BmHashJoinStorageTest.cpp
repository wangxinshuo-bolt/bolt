#include "bolt/exec/BmHashJoinStorage.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/exec/BmHashTable.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {

struct BmRowContainerTestPeer {
  static uint64_t partitionGeneration(
      const BmRowContainer& container,
      PartitionId partition) {
    return container.partitionLeaseStates_[partition]->generation;
  }

  static uint32_t partitionLeaseCount(
      const BmRowContainer& container,
      PartitionId partition) {
    return container.partitionLeaseStates_[partition]->activeLeaseCount;
  }
};

} // namespace bytedance::bolt::exec::bm

namespace bytedance::bolt::exec::test {
namespace {

class BmHashJoinStorageTest : public testing::Test,
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

    const auto directory =
        memory::bm::test::UniqueTempDir(root_->name());
    std::filesystem::remove_all(directory);
    memory::bm::BufferManagerConfig config;
    config.poolName = root_->name();
    config.spillStoreConfig.fileAllocatorConfig =
        memory::bm::test::ValidConfigWithDirectory(directory);
    config.spillStoreConfig.fileAllocatorConfig.max_open_files_per_bucket = 64;
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

  std::vector<TypePtr> childTypes(const RowVectorPtr& input) {
    std::vector<TypePtr> types;
    types.reserve(input->childrenSize());
    for (auto i = 0; i < input->childrenSize(); ++i) {
      types.push_back(input->childAt(i)->type());
    }
    return types;
  }

  std::shared_ptr<BmHashJoinStorage> makeStorage(
      const RowVectorPtr& input,
      int32_t numKeys,
      bool allowDuplicates = true,
      bool hasProbedFlag = true,
      uint32_t rowBlockSize = 4 << 10,
      uint32_t heapBlockSize = 4 << 10) {
    return BmHashJoinStorage::createForJoin(
        childTypes(input),
        numKeys,
        allowDuplicates,
        hasProbedFlag,
        bufferManager_,
        rowBlockSize,
        heapBlockSize);
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
      int32_t numKeys,
      std::shared_ptr<BmHashJoinStorage> storage = nullptr) {
    std::vector<TypePtr> dependents;
    for (auto i = numKeys; i < type->size(); ++i) {
      dependents.push_back(type->childAt(i));
    }
    if (storage) {
      return BmTable::createForJoin(
          makeHashers(type, numKeys),
          dependents,
          true,
          false,
          0,
          pool(),
          std::move(storage),
          false);
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

  void appendRowsToStorage(
      BmHashJoinStorage& storage,
      const RowVectorPtr& input) {
    SelectivityVector rows(input->size());
    std::vector<DecodedVector> decodedVectors;
    decodedVectors.reserve(input->childrenSize());
    for (const auto& child : input->children()) {
      decodedVectors.emplace_back(*child, rows);
    }

    rows.applyToSelected([&](auto rowIndex) {
      auto context = storage.rows().appendRow();
      for (auto column = 0; column < decodedVectors.size(); ++column) {
        storage.rows().store(context, decodedVectors[column], rowIndex, column);
      }
    });
  }

  std::vector<std::string> extractRows(
      BmHashJoinStorage& storage,
      folly::Range<char* const*> rows,
      const RowTypePtr& type) {
    std::vector<VectorPtr> columns;
    columns.reserve(type->size());
    for (auto column = 0; column < type->size(); ++column) {
      auto result = BaseVector::create(type->childAt(column), rows.size(), pool());
      storage.rows().extractColumnResident(rows.data(), rows.size(), column, result);
      columns.push_back(result);
    }

    std::vector<std::string> serialized;
    serialized.reserve(rows.size());
    for (auto row = 0; row < rows.size(); ++row) {
      std::string value;
      for (auto column = 0; column < columns.size(); ++column) {
        if (column > 0) {
          value.append("|");
        }
        value.append(columns[column]->toString(row));
      }
      serialized.push_back(std::move(value));
    }
    return serialized;
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
    EXPECT_NE(buildType, nullptr);
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

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
};

TEST_F(BmHashJoinStorageTest, SpillReloadTracksBmIoAcrossRepeatedReloads) {
  constexpr vector_size_t kRows = 4096;
  auto makeBatch = [&](vector_size_t begin, vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int64_t>(
            size, [begin](auto row) { return (begin + row) % 128; }),
        makeFlatVector<std::string>(size, [begin](auto row) {
          return fmt::format("spill-row-{:04}", begin + row);
        }),
        makeFlatVector<int64_t>(
            size, [begin](auto row) { return (begin + row) * 10; }),
    });
  };
  auto input = makeBatch(0, kRows);
  const auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
  ASSERT_NE(rowType, nullptr);

  auto storage = makeStorage(input, 1, true, true, 4 << 10, 4 << 10);
  appendRowsToStorage(*storage, makeBatch(0, 1536));
  storage->rows().spillActivePartitionSegment(bm::kDefaultPartition);
  appendRowsToStorage(*storage, makeBatch(1536, 1536));
  storage->rows().spillActivePartitionSegment(bm::kDefaultPartition);
  appendRowsToStorage(*storage, makeBatch(3072, 1024));

  storage->spillPartition();
  const auto& spillStats = storage->runtimeStats();
  EXPECT_EQ(kRows, spillStats.spilledRows);
  EXPECT_GT(spillStats.spilledSegments, 1);
  EXPECT_GT(spillStats.spilledBytes, 0);
  EXPECT_GT(spillStats.spillWriteCount, 0);
  EXPECT_GT(spillStats.spillWriteBytes, 0);
  EXPECT_GT(spillStats.spillPhysicalWriteBytes, 0);

  uint64_t firstReadCount = 0;
  {
    auto firstReload = storage->loadPartition();
    ASSERT_EQ(kRows, firstReload.rows.size());
    EXPECT_TRUE(firstReload.lease.active());
    auto firstRows = extractRows(
        *storage,
        {firstReload.rows.data(), firstReload.rows.size()},
        rowType);
    EXPECT_EQ("0|spill-row-0000|0", firstRows.front());
    EXPECT_EQ("127|spill-row-4095|40950", firstRows.back());
    EXPECT_EQ(1, storage->runtimeStats().restoreCount);
    EXPECT_GT(storage->runtimeStats().spillReadCount, 0);
    EXPECT_GT(storage->runtimeStats().spillReadBytes, 0);
    EXPECT_GT(storage->runtimeStats().spillPhysicalReadBytes, 0);

    storage->rows().setNext(firstReload.rows.front(), firstReload.rows.back());
    storage->rows().setProbed(firstReload.rows.front(), true);
    firstReadCount = firstReload.stats.spillReadCount;
  }

  auto secondReload = storage->loadPartition();
  ASSERT_EQ(kRows, secondReload.rows.size());
  EXPECT_EQ(nullptr, storage->rows().next(secondReload.rows.front()));
  EXPECT_FALSE(storage->rows().probed(secondReload.rows.front()));
  EXPECT_EQ(2, storage->runtimeStats().restoreCount);
  EXPECT_GE(storage->runtimeStats().spillReadCount, firstReadCount);
}

TEST_F(BmHashJoinStorageTest, RebuildAfterReloadPreservesDuplicateProbeParity) {
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
  auto storage = makeStorage(build, 1, true, false, 512, 512);
  auto bm = makeBmTable(buildType, 1, storage);
  appendBuildRows(*legacy, build, 1);
  appendBuildRows(*bm, build, 1);

  bm->spillPartition();
  bm->reloadFromStorage();

  const auto legacyResults = probeAndCollect(*legacy, build, probe);
  const auto bmResults = probeAndCollect(*bm, build, probe);
  EXPECT_EQ(legacyResults.selectedRows, bmResults.selectedRows);
  EXPECT_EQ(legacyResults.hasFirstHit, bmResults.hasFirstHit);
  EXPECT_EQ(legacyResults.hitCounts, bmResults.hitCounts);
  EXPECT_EQ(legacyResults.resultRows, bmResults.resultRows);

  bm->spillPartition();
  bm->reloadFromStorage();
  const auto secondResults = probeAndCollect(*bm, build, probe);
  EXPECT_EQ(legacyResults.selectedRows, secondResults.selectedRows);
  EXPECT_EQ(legacyResults.hasFirstHit, secondResults.hasFirstHit);
  EXPECT_EQ(legacyResults.hitCounts, secondResults.hitCounts);
  EXPECT_EQ(legacyResults.resultRows, secondResults.resultRows);
}

TEST_F(BmHashJoinStorageTest, TableOwnsStorageAndReleasesLeaseOnDestroy) {
  const auto build = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 3}),
      makeFlatVector<std::string>({"one-a", "one-b", "two", "three"}),
      makeFlatVector<int64_t>({10, 11, 20, 30}),
  });
  const auto probe = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 4}),
  });
  const auto buildType = std::dynamic_pointer_cast<const RowType>(build->type());
  ASSERT_NE(buildType, nullptr);

  auto storage = makeStorage(build, 1, true, false, 512, 512);
  auto weakStorage = std::weak_ptr<BmHashJoinStorage>(storage);
  {
    auto bm = makeBmTable(buildType, 1, storage);
    appendBuildRows(*bm, build, 1);
    bm->spillPartition();
    bm->reloadFromStorage();

    storage.reset();
    ASSERT_FALSE(weakStorage.expired());
    auto retained = weakStorage.lock();
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(
        1,
        bm::BmRowContainerTestPeer::partitionLeaseCount(
            retained->rows(), bm::kDefaultPartition));

    const auto results = probeAndCollect(*bm, build, probe);
    EXPECT_EQ(std::vector<vector_size_t>({0, 1, 2}), results.selectedRows);
    EXPECT_EQ(std::vector<bool>({true, true, false}), results.hasFirstHit);
  }

  EXPECT_TRUE(weakStorage.expired());
}

} // namespace
} // namespace bytedance::bolt::exec::test
