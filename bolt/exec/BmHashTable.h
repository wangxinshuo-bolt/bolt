#pragma once

#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/bm/BmRowContainer.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace bytedance::bolt::exec {

namespace test {
class BmHashTableTest;
}

template <bool ignoreNullKeys>
class BmHashTable : public BaseHashTable {
 public:
  struct RuntimeStats {
    uint64_t bmRows{0};
    uint64_t spillBytes{0};
    uint64_t spillSegments{0};
    uint64_t restoreCount{0};
  };

  static std::unique_ptr<BmHashTable> createForJoin(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      const std::vector<TypePtr>& dependentTypes,
      bool allowDuplicates,
      bool hasProbedFlag,
      uint32_t minTableSizeForParallelJoinBuild,
      memory::MemoryPool* pool,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      bool jitRowEqVectors) {
    return std::make_unique<BmHashTable>(
        std::move(hashers),
        dependentTypes,
        allowDuplicates,
        hasProbedFlag,
        minTableSizeForParallelJoinBuild,
        pool,
        std::move(bufferManager),
        jitRowEqVectors);
  }

  BmHashTable(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      const std::vector<TypePtr>& dependentTypes,
      bool allowDuplicates,
      bool hasProbedFlag,
      uint32_t minTableSizeForParallelJoinBuild,
      memory::MemoryPool* pool,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      bool jitRowEqVectors);

  ~BmHashTable() override = default;

  void groupProbe(HashLookup& lookup) override;
  void joinProbe(HashLookup& lookup) override;
  void directAddRows(
      HashLookup& lookup,
      const RowVectorPtr& input,
      SelectivityVector& rows,
      bool decodeAndRemoveNulls) override;

  void appendJoinRows(
      const SelectivityVector& rows,
      folly::Range<const DecodedVector* const*> keyDecoders,
      folly::Range<const DecodedVector* const*> dependentDecoders) override;

  void extractJoinColumn(
      const char* const* rows,
      int32_t numRows,
      int32_t column,
      const VectorPtr& result) const override;

  void setJoinProbedFlags(char* const* rows, int32_t numRows) override;

  void extractJoinProbedFlags(
      const char* const* rows,
      int32_t numRows,
      bool setNullForNullKeysRow,
      bool setNullForNonProbedRow,
      const VectorPtr& result) const override;

  uint64_t joinRowCount() const override;

  int32_t listJoinResults(
      JoinResultIterator& iter,
      bool includeMisses,
      folly::Range<vector_size_t*> inputRows,
      folly::Range<char**> hits,
      const BaseVector* matchFlags = nullptr) override;

  int32_t listNotProbedRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) override;

  int32_t listProbedRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) override;

  int32_t listAllRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) override;

  int32_t listNullKeyRows(
      NullKeyRowsIterator* iter,
      int32_t maxRows,
      char** rows) override;

  void prepareJoinTable(
      std::vector<std::unique_ptr<BaseHashTable>> tables,
      folly::Executor* executor = nullptr,
      bool dropDuplicates = false,
      int8_t spillInputStartPartitionBit =
          kNoSpillInputStartPartitionBit) override;

  void joinTableMayHaveDuplicates() override {
    joinBuildNoDuplicates_ = false;
  }

  int64_t allocatedBytes() const override;

  void clear() override;

  uint64_t capacity() const override {
    return capacity_;
  }

  uint64_t numDistinct() const override {
    return numDistinctKeys_;
  }

  float getDistinctRatio() const override {
    return numProbeInputs_ == 0 ? 0 : numDistinctKeys_ * 1.0 / numProbeInputs_;
  }

  HashTableStats stats() const override {
    return HashTableStats{
        static_cast<int64_t>(capacity_),
        static_cast<int64_t>(numRehashes_),
        static_cast<int64_t>(numDistinctKeys_),
        0};
  }

  uint64_t hashTableSizeIncrease(int32_t numNewDistinct) const override;
  uint64_t estimateHashTableSize(uint64_t numDistinct) const override;

  bool hasDuplicateKeys() const override {
    return hasDuplicates_.check();
  }

  HashMode hashMode() const override {
    return HashMode::kHash;
  }

  void decideHashMode(int32_t numNew, bool disableRangeArrayHash = false)
      override;

  void erase(folly::Range<char**> rows) override;

  std::string toString() override;

  std::vector<RowContainer*> allRows() const override;
  std::vector<HybridContainer*> allHybridContainers() const override {
    return {};
  }

  HashStringAllocator* stringAllocator() override;

  const RuntimeStats& runtimeStats() const {
    return runtimeStats_;
  }

  const bm::BmRowContainer& bmRows() const {
    return *bmRows_;
  }

 private:
  static constexpr bool kTestingHashOverrideEnabled = false;

  struct DistinctHead {
    char* head{nullptr};
  };

  struct BucketEntry {
    uint64_t hash{0};
    std::vector<DistinctHead> heads;
  };

  void setHashMode(HashMode mode, int32_t numNew) override;
  int sizeBits() const override;

  static std::vector<bool> makeNullable(const std::vector<TypePtr>& types);
  static std::vector<TypePtr> makeAllTypes(
      const std::vector<std::unique_ptr<VectorHasher>>& hashers,
      const std::vector<TypePtr>& dependentTypes);

  void unsupported(folly::StringPiece method) const;
  uint64_t maybeApplyTestHashOverride(uint64_t hash) const;
  void testingSetHashOverride(uint64_t (*override)(uint64_t));
  void maybeGrowDirectory(uint64_t requiredRows);
  uint64_t nextCapacity(uint64_t requiredRows) const;
  char* insertOrFindGroup(char* row, uint64_t hash, bool& insertedNewKey);
  bool rowsEqualOnKeys(const char* left, const char* right) const;
  bool rowMatchesLookup(
      const char* row,
      HashLookup& lookup,
      vector_size_t probeRow) const;
  BucketEntry& bucketForHash(uint64_t hash);
  const BucketEntry* bucketForHash(uint64_t hash) const;

  uint32_t minTableSizeForParallelJoinBuild_;
  memory::MemoryPool* pool_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  std::unique_ptr<bm::BmRowContainer> bmRows_;
  std::vector<TypePtr> allTypes_;
  std::vector<TypePtr> dependentTypes_;
  bool joinBuildNoDuplicates_{false};
  bool hasProbedFlag_{false};
  bool enableJit_{false};
  uint64_t (*testHashOverride_)(uint64_t){nullptr};

  std::unordered_map<uint64_t, BucketEntry> buckets_;
  uint64_t capacity_{0};
  uint64_t numRows_{0};
  uint64_t numDistinctKeys_{0};
  uint64_t numRehashes_{0};
  uint64_t numProbeInputs_{0};
  OneWayStatusFlag hasDuplicates_;
  RuntimeStats runtimeStats_;

  friend class test::BmHashTableTest;
};

extern template class BmHashTable<true>;

} // namespace bytedance::bolt::exec
