#include "bolt/exec/BmHashTable.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace bytedance::bolt::exec {
namespace {

constexpr uint64_t kInitialCapacity = 2048;
constexpr double kMaxLoadFactor = 0.75;

} // namespace

template <bool ignoreNullKeys>
std::vector<bool> BmHashTable<ignoreNullKeys>::makeNullable(
    const std::vector<TypePtr>& types) {
  return std::vector<bool>(types.size(), true);
}

template <bool ignoreNullKeys>
std::vector<TypePtr> BmHashTable<ignoreNullKeys>::makeAllTypes(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    const std::vector<TypePtr>& dependentTypes) {
  std::vector<TypePtr> types;
  types.reserve(hashers.size() + dependentTypes.size());
  for (const auto& hasher : hashers) {
    types.push_back(hasher->type());
  }
  types.insert(types.end(), dependentTypes.begin(), dependentTypes.end());
  return types;
}

template <bool ignoreNullKeys>
BmHashTable<ignoreNullKeys>::BmHashTable(
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    const std::vector<TypePtr>& dependentTypes,
    bool allowDuplicates,
    bool hasProbedFlag,
    uint32_t minTableSizeForParallelJoinBuild,
    memory::MemoryPool* pool,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    bool jitRowEqVectors,
    HashOverride hashOverride)
    : BaseHashTable(std::move(hashers)),
      minTableSizeForParallelJoinBuild_(minTableSizeForParallelJoinBuild),
      pool_(pool),
      bufferManager_(std::move(bufferManager)),
      allTypes_(makeAllTypes(hashers_, dependentTypes)),
      dependentTypes_(dependentTypes),
      joinBuildNoDuplicates_(!allowDuplicates),
      hasProbedFlag_(hasProbedFlag),
      enableJit_(jitRowEqVectors),
      hashOverride_(std::move(hashOverride)) {
  BOLT_CHECK(ignoreNullKeys, "Task 5 only supports ignore-null-key inner join");
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK_NOT_NULL(bufferManager_);
  bm::BmJoinLayoutOptions options{
      .numKeys = static_cast<uint32_t>(hashers_.size()),
      .hasNext = allowDuplicates,
      .hasProbedFlag = hasProbedFlag,
      .hasNormalizedKey = false,
  };
  bmRows_ = std::make_unique<bm::BmRowContainer>(
      allTypes_,
      makeNullable(allTypes_),
      bufferManager_,
      memory::bm::MemoryTag::kHashBuild,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)),
      options);
  capacity_ = kInitialCapacity;
  buckets_.reserve(capacity_);
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::unsupported(folly::StringPiece method) const {
  BOLT_UNSUPPORTED(
      "BmHashTable Task5 resident backend does not support {} before Task6",
      method);
}

template <bool ignoreNullKeys>
uint64_t BmHashTable<ignoreNullKeys>::applyHashOverride(uint64_t hash) const {
  return hashOverride_ ? (*hashOverride_)(hash) : hash;
}

template <bool ignoreNullKeys>
uint64_t BmHashTable<ignoreNullKeys>::nextCapacity(uint64_t requiredRows) const {
  uint64_t capacity = std::max<uint64_t>(kInitialCapacity, capacity_);
  const auto requiredDistinct =
      static_cast<uint64_t>(std::ceil(requiredRows / kMaxLoadFactor));
  while (capacity < requiredDistinct) {
    capacity *= 2;
  }
  return capacity;
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::maybeGrowDirectory(uint64_t requiredRows) {
  if (requiredRows <= static_cast<uint64_t>(capacity_ * kMaxLoadFactor)) {
    return;
  }
  capacity_ = nextCapacity(requiredRows);
  ++numRehashes_;
  buckets_.reserve(capacity_);
}

template <bool ignoreNullKeys>
auto BmHashTable<ignoreNullKeys>::bucketForHash(uint64_t hash) -> BucketEntry& {
  auto [it, inserted] = buckets_.try_emplace(hash, BucketEntry{.hash = hash});
  if (inserted) {
    maybeGrowDirectory(numDistinctKeys_ + 1);
  }
  return it->second;
}

template <bool ignoreNullKeys>
auto BmHashTable<ignoreNullKeys>::bucketForHash(uint64_t hash) const
    -> const BucketEntry* {
  auto it = buckets_.find(hash);
  return it == buckets_.end() ? nullptr : &it->second;
}

template <bool ignoreNullKeys>
bool BmHashTable<ignoreNullKeys>::rowsEqualOnKeys(
    const char* left,
    const char* right) const {
  for (auto i = 0; i < hashers_.size(); ++i) {
    if (bmRows_->compare(left, right, i, CompareFlags{true, true}) != 0) {
      return false;
    }
  }
  return true;
}

template <bool ignoreNullKeys>
bool BmHashTable<ignoreNullKeys>::rowMatchesLookup(
    const char* row,
    HashLookup& lookup,
    vector_size_t probeRow) const {
  for (auto i = 0; i < lookup.hashers.size(); ++i) {
    if (!bmRows_->equalsDecoded(
            row,
            i,
            lookup.hashers[i]->decodedVector(),
            probeRow,
            !ignoreNullKeys)) {
      return false;
    }
  }
  return true;
}

template <bool ignoreNullKeys>
char* BmHashTable<ignoreNullKeys>::insertOrFindGroup(
    char* row,
    uint64_t hash,
    bool& insertedNewKey) {
  auto& bucket = bucketForHash(hash);
  for (auto& distinct : bucket.heads) {
    if (rowsEqualOnKeys(distinct.head, row)) {
      insertedNewKey = false;
      if (bmRows_->next(row) != nullptr) {
        BOLT_FAIL("New BM join row must not have a pre-linked next pointer");
      }
      if (bmRows_->next(distinct.head) == nullptr) {
        hasDuplicates_.set();
      }
      bmRows_->setNext(row, bmRows_->next(distinct.head));
      bmRows_->setNext(distinct.head, row);
      return distinct.head;
    }
  }

  insertedNewKey = true;
  bucket.heads.push_back(DistinctHead{.head = row});
  ++numDistinctKeys_;
  maybeGrowDirectory(numDistinctKeys_);
  return row;
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::appendJoinRows(
    const SelectivityVector& rows,
    folly::Range<const DecodedVector* const*> keyDecoders,
    folly::Range<const DecodedVector* const*> dependentDecoders) {
  BOLT_CHECK_EQ(keyDecoders.size(), hashers_.size());
  auto rowCountBefore = bmRows_->numRows();
  std::vector<char*> appended;
  appended.reserve(rows.countSelected());

  rows.applyToSelected([&](auto rowIndex) {
    auto context = bmRows_->appendRow();
    for (auto i = 0; i < keyDecoders.size(); ++i) {
      bmRows_->store(context, *keyDecoders[i], rowIndex, i);
    }
    for (auto i = 0; i < dependentDecoders.size(); ++i) {
      bmRows_->store(
          context, *dependentDecoders[i], rowIndex, i + keyDecoders.size());
    }
    appended.push_back(context.row());
  });

  raw_vector<uint64_t> hashes;
  hashes.resize(appended.size());
  std::fill(hashes.begin(), hashes.end(), 0);
  std::vector<int32_t> keyColumns(hashers_.size());
  std::iota(keyColumns.begin(), keyColumns.end(), 0);
  bmRows_->hashRows(appended, keyColumns, hashes);

  for (auto i = 0; i < appended.size(); ++i) {
    bool insertedNewKey = false;
    auto hash = applyHashOverride(hashes[i]);
    insertOrFindGroup(appended[i], hash, insertedNewKey);
  }

  numRows_ += appended.size();
  runtimeStats_.bmRows = bmRows_->numRows();
  BOLT_CHECK_EQ(
      bmRows_->numRows(),
      rowCountBefore + static_cast<int64_t>(appended.size()));
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::joinProbe(HashLookup& lookup) {
  numProbeInputs_ += lookup.rows.size();
  for (auto row : lookup.rows) {
    auto hash = applyHashOverride(lookup.hashes[row]);
    const auto it = buckets_.find(hash);
    if (it == buckets_.end()) {
      lookup.hits[row] = nullptr;
      continue;
    }
    const auto& bucket = it->second;

    char* match = nullptr;
    for (const auto& distinct : bucket.heads) {
      if (rowMatchesLookup(distinct.head, lookup, row)) {
        match = distinct.head;
        break;
      }
    }
    lookup.hits[row] = match;
  }
}

template <bool ignoreNullKeys>
int32_t BmHashTable<ignoreNullKeys>::listJoinResults(
    JoinResultIterator& iter,
    bool includeMisses,
    folly::Range<vector_size_t*> inputRows,
    folly::Range<char**> hits,
    const BaseVector* matchFlags) {
  BOLT_CHECK_LE(inputRows.size(), hits.size());
  BOLT_CHECK(
      matchFlags == nullptr,
      "BmHashTable Task5 does not support matchFlags in listJoinResults");

  int32_t numOut = 0;
  const auto maxOut = static_cast<int32_t>(inputRows.size());
  while (iter.lastRowIndex < iter.rows->size()) {
    if (!iter.nextHit) {
      auto row = (*iter.rows)[iter.lastRowIndex];
      iter.nextHit = (*iter.hits)[row];
      if (!iter.nextHit) {
        ++iter.lastRowIndex;
        if (!includeMisses) {
          continue;
        }
        inputRows[numOut] = row;
        hits[numOut] = nullptr;
        ++numOut;
        if (numOut >= maxOut) {
          return numOut;
        }
        continue;
      }
    }

    while (iter.nextHit) {
      const auto row = (*iter.rows)[iter.lastRowIndex];
      auto* current = iter.nextHit;
      inputRows[numOut] = row;
      hits[numOut] = current;
      ++numOut;
      iter.nextHit = bmRows_->next(current);
      if (!iter.nextHit) {
        ++iter.lastRowIndex;
      }
      if (numOut >= maxOut) {
        return numOut;
      }
    }
  }
  return numOut;
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::extractJoinColumn(
    const char* const* rows,
    int32_t numRows,
    int32_t column,
    const VectorPtr& result) const {
  bmRows_->extractColumnResident(rows, numRows, column, result);
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::setJoinProbedFlags(
    char* const* rows,
    int32_t numRows) {
  BOLT_CHECK(
      hasProbedFlag_,
      "BmHashTable does not have probed flag storage for this join layout");
  for (auto i = 0; i < numRows; ++i) {
    if (rows[i] != nullptr) {
      bmRows_->setProbed(rows[i], true);
    }
  }
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::extractJoinProbedFlags(
    const char* const* rows,
    int32_t numRows,
    bool setNullForNullKeysRow,
    bool setNullForNonProbedRow,
    const VectorPtr& result) const {
  BOLT_CHECK(
      hasProbedFlag_,
      "BmHashTable does not have probed flag storage for this join layout");
  auto* flat = result->asFlatVector<bool>();
  BOLT_CHECK_NOT_NULL(flat);
  result->resize(numRows);
  auto values = flat->mutableValues(numRows)->asMutableRange<bool>();
  auto* nulls = result->mutableRawNulls();
  for (auto i = 0; i < numRows; ++i) {
    if (rows[i] == nullptr) {
      bits::setNull(nulls, i, setNullForNullKeysRow || setNullForNonProbedRow);
      values[i] = false;
      continue;
    }
    const auto probed = bmRows_->probed(rows[i]);
    bits::setNull(nulls, i, setNullForNonProbedRow && !probed);
    values[i] = probed;
  }
}

template <bool ignoreNullKeys>
uint64_t BmHashTable<ignoreNullKeys>::joinRowCount() const {
  return numRows_;
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::prepareJoinTable(
    std::vector<std::unique_ptr<BaseHashTable>> tables,
    folly::Executor* executor,
    bool dropDuplicates,
    int8_t spillInputStartPartitionBit) {
  BOLT_CHECK(
      tables.empty(),
      "BmHashTable Task5 resident backend does not support table merge");
  BOLT_CHECK_NULL(executor);
  BOLT_CHECK(
      !dropDuplicates,
      "BmHashTable Task5 resident backend does not support dropDuplicates");
  checkHashBitsOverlap(spillInputStartPartitionBit);
  runtimeStats_.bmRows = bmRows_->numRows();
}

template <bool ignoreNullKeys>
int64_t BmHashTable<ignoreNullKeys>::allocatedBytes() const {
  const auto bmStats = bufferManager_->stats();
  const auto directoryBytes = static_cast<uint64_t>(
      buckets_.size() * sizeof(typename decltype(buckets_)::value_type) +
      numDistinctKeys_ * sizeof(DistinctHead));
  return static_cast<int64_t>(
      directoryBytes + bmStats.pinnedResidentBytes + bmStats.unpinnedResidentBytes +
      bmStats.spilledBytes);
}

template <bool ignoreNullKeys>
uint64_t BmHashTable<ignoreNullKeys>::hashTableSizeIncrease(
    int32_t numNewDistinct) const {
  const auto next = nextCapacity(numDistinctKeys_ + numNewDistinct);
  if (next <= capacity_) {
    return 0;
  }
  return (next - capacity_) * sizeof(typename decltype(buckets_)::value_type);
}

template <bool ignoreNullKeys>
uint64_t BmHashTable<ignoreNullKeys>::estimateHashTableSize(
    uint64_t numDistinct) const {
  const auto next = nextCapacity(numDistinct);
  return next * sizeof(typename decltype(buckets_)::value_type);
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::clear() {
  unsupported("clear()");
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::decideHashMode(
    int32_t numNew,
    bool disableRangeArrayHash) {
  BOLT_CHECK_EQ(hashMode(), HashMode::kHash);
  BOLT_CHECK_EQ(disableRangeArrayHash, false);
  maybeGrowDirectory(numDistinctKeys_ + numNew);
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::erase(folly::Range<char**> rows) {
  unsupported("erase()");
}

template <bool ignoreNullKeys>
std::string BmHashTable<ignoreNullKeys>::toString() {
  return fmt::format(
      "BmHashTable<HashMode::kHash>[rows={}, distinctKeys={}, buckets={}, capacity={}]",
      numRows_,
      numDistinctKeys_,
      buckets_.size(),
      capacity_);
}

template <bool ignoreNullKeys>
std::vector<RowContainer*> BmHashTable<ignoreNullKeys>::allRows() const {
  unsupported("allRows()");
  return {};
}

template <bool ignoreNullKeys>
HashStringAllocator* BmHashTable<ignoreNullKeys>::stringAllocator() {
  unsupported("stringAllocator()");
  return nullptr;
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::groupProbe(HashLookup& lookup) {
  unsupported("groupProbe()");
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::directAddRows(
    HashLookup& lookup,
    const RowVectorPtr& input,
    SelectivityVector& rows,
    bool decodeAndRemoveNulls) {
  unsupported("directAddRows()");
}

template <bool ignoreNullKeys>
int32_t BmHashTable<ignoreNullKeys>::listNotProbedRows(
    RowsIterator* iter,
    int32_t maxRows,
    uint64_t maxBytes,
    char** rows) {
  unsupported("listNotProbedRows()");
  return 0;
}

template <bool ignoreNullKeys>
int32_t BmHashTable<ignoreNullKeys>::listProbedRows(
    RowsIterator* iter,
    int32_t maxRows,
    uint64_t maxBytes,
    char** rows) {
  unsupported("listProbedRows()");
  return 0;
}

template <bool ignoreNullKeys>
int32_t BmHashTable<ignoreNullKeys>::listAllRows(
    RowsIterator* iter,
    int32_t maxRows,
    uint64_t maxBytes,
    char** rows) {
  unsupported("listAllRows()");
  return 0;
}

template <bool ignoreNullKeys>
int32_t BmHashTable<ignoreNullKeys>::listNullKeyRows(
    NullKeyRowsIterator* iter,
    int32_t maxRows,
    char** rows) {
  unsupported("listNullKeyRows()");
  return 0;
}

template <bool ignoreNullKeys>
void BmHashTable<ignoreNullKeys>::setHashMode(HashMode mode, int32_t numNew) {
  BOLT_CHECK_EQ(mode, HashMode::kHash);
  maybeGrowDirectory(numDistinctKeys_ + numNew);
}

template <bool ignoreNullKeys>
int BmHashTable<ignoreNullKeys>::sizeBits() const {
  if (capacity_ == 0) {
    return 0;
  }
  BOLT_CHECK_EQ(bits::nextPowerOfTwo(capacity_), capacity_);
  return 63 - bits::countLeadingZeros(capacity_);
}

template class BmHashTable<true>;

} // namespace bytedance::bolt::exec
