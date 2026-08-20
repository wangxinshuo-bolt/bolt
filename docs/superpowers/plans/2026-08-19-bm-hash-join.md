# BmRowContainer HashJoin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, serial Bolt inner HashJoin whose build rows, build-side spill, and reload are owned by `BmRowContainer`, then prove it end-to-end from Gluten for multiple scalar types, non-spill, spill, and different spill volumes.

**Architecture:** Keep the existing `HashJoinNode`, `HashBuild`, `HashProbe`, and `HashJoinBridge` pipeline. Add a capability-gated `BmHashTable<ignoreNullKeys>` generic-hash backend that stores build rows in a join-capable `BmRowContainer`; keep all unsupported shapes on the legacy `HashTable`. Build-side spill seals and flushes BM segments, reloads them into a new resident pointer epoch before table publication, and rebuilds all pointer-bearing state; probe-side extraction goes through storage-neutral `BaseHashTable` batch methods.

**Tech Stack:** C++20, Bolt execution engine, BufferManager/BmRowContainer, CMake/Conan presets, GoogleTest, Gluten C++ JNI backend, Scala/Spark SQL/ScalaTest, Maven.

**Spec:** `BM_HASH_JOIN_INTEGRATION_INVESTIGATION.md`

## Global Constraints

- The feature is disabled by default and selected only when `bm-hash-join-enabled=true`.
- The first release supports Gluten Bolt only, `Task::ExecutionMode::kSerial`, no supplied QueryCtx driver executor, ungrouped, non-morsel, one HashBuild driver, one HashProbe driver, inner join, generic hash, no reusable table, and no hybrid join.
- Capability rejection happens before the first build row is written and falls back to the legacy HashJoin. After the first BM row is written, errors fail the query; BM state is never replayed into legacy storage.
- Build rows and build-side spill/reload are BM-owned. The legacy build `Spiller` must not serialize BM build rows. Existing probe-side spill remains in use.
- A `char*` is valid only inside one resident epoch. Before unpin/spill, destroy every table/index/iterator that can retain it; after reload, reset runtime metadata and rebuild the table.
- `RoundLease` is a logical release gate over the existing `BlockRef.handle` pins; it must not acquire a second physical pin.
- The first release supports scalar BmRowContainer types: BOOLEAN, TINYINT, SMALLINT, INTEGER/DATE, BIGINT/short decimal, HUGEINT/long decimal, REAL, DOUBLE, TIMESTAMP, VARCHAR, and VARBINARY. Complex keys and payloads fall back before commit.
- Array, normalized-key, JIT row equality, parallel build, recursive repartition, skew/range partitioning, reusable tables, and non-inner join types remain legacy.
- The initial spill implementation may reload all sealed segments for a single logical build partition before publishing. It must perform real BM writes and reads, expose BM spill/restore metrics, and fail clearly if the complete partition cannot be admitted.
- The Bolt and Gluten directories are separate Git repositories. Commit Bolt changes in `/data00/home/wangxinshuo.db/bolt`; commit Gluten changes in `/data00/home/wangxinshuo.db/bolt/gluten`; never stage `READY.txt` or the nested `gluten/` directory in the Bolt repository.
- Commit after each independently testable task and do not push.

## File and Interface Map

- `bolt/exec/bm/BmRowLayout.{h,cpp}`: persisted cells plus inline join runtime metadata offsets.
- `bolt/exec/bm/BmRowContainer*.cpp`: selected append, equality/hash, extraction, row listing, segment sealing/reload, and the lease-aware release primitive.
- `bolt/exec/BmHashTable.{h,cpp}`: serial generic-hash buckets, duplicate chains, probe, result listing, and BM metrics.
- `bolt/exec/HashTable.{h,cpp}`: storage-neutral build append/extract/probed batch contract; legacy implementation remains behaviorally unchanged.
- `bolt/exec/HashBuild.{h,cpp}`: capability gate, backend factory, BM build append, optional segment flush, reload, and publication.
- `bolt/exec/HashProbe.{h,cpp}`: storage-neutral extraction and explicit round-pointer invalidation.
- `bolt/exec/HashJoinBridge.{h,cpp}`: BM storage ownership and lock-outside table destruction.
- `bolt/core/QueryConfig.h`: native feature gate.
- `gluten/backends-bolt/.../BoltConfig.scala`, `gluten/cpp/bolt/config/BoltConfig.h`, and `WholeStageResultIterator.cc`: Spark-to-Bolt configuration mapping and serial capability context.
- `gluten/backends-bolt/src/test/scala/org/apache/gluten/execution/BoltHashJoinSuite.scala`: Spark E2E correctness, backend selection, and spill-volume assertions.

---

### Task 1: Break the BM storage link cycle

**Files:**
- Modify: `bolt/exec/bm/CMakeLists.txt`
- Modify: `bolt/exec/CMakeLists.txt`
- Modify: `bolt/exec/bm/tests/CMakeLists.txt`
- Create: `bolt/exec/bm/tests/BmStorageLinkTest.cpp`

**Interfaces:**
- Produces: `bolt_exec_bm_storage`, a library containing `BmRowContainer` and segment/read implementations and linking only `bolt_memory_bm`, `bolt_vector`, and `bolt_common_base`.
- Produces: compatibility target `bolt_exec_bm` linking `bolt_exec_bm_storage` and `bolt_exec` for existing tests/benchmarks.
- Consumed by: `bolt_exec`, which links `bolt_exec_bm_storage` before `BmHashTable` is added.

- [ ] **Step 1: Add a link smoke test**

  Add `BmStorageLinkTest.cpp` that constructs a `BmRowLayout` for `{BIGINT(), VARCHAR()}` and asserts non-zero row size. Link a new `bolt_exec_bm_storage_link_test` only to `bolt_exec_bm_storage`, `bolt_memory_bm`, and GTest; it must not link `bolt_exec`.

- [ ] **Step 2: Verify RED at configure/build time**

  Run `make release_with_test BOLT_CONAN_CONFIGURE_ONLY=1`, then `cmake --build --preset conan-release --target bolt_exec_bm_storage_link_test`. Expected: configure or build fails because `bolt_exec_bm_storage` does not exist.

- [ ] **Step 3: Split the target**

  Move all current storage source files into `bolt_exec_bm_storage`. Define `bolt_exec_bm` as an interface compatibility target, link `bolt_exec` to `bolt_exec_bm_storage`, and keep tests/benchmarks resolving their old target name.

- [ ] **Step 4: Verify GREEN and legacy targets**

  Run:

  ```bash
  cmake --build --preset conan-release --target bolt_exec_bm_storage_link_test bolt_exec_bm_test bolt_exec_hash_join_test
  _build/Release/bolt/exec/bm/tests/bolt_exec_bm_storage_link_test --gtest_color=no
  _build/Release/bolt/exec/bm/tests/bolt_exec_bm_test --gtest_color=no
  ```

  Expected: link smoke passes and all 44 baseline BM tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add bolt/exec/CMakeLists.txt bolt/exec/bm/CMakeLists.txt bolt/exec/bm/tests/CMakeLists.txt bolt/exec/bm/tests/BmStorageLinkTest.cpp
  git commit -m "build: split BM row storage target"
  ```

### Task 2: Add join layout metadata and selected append

**Files:**
- Modify: `bolt/exec/bm/BmRowLayout.h`
- Modify: `bolt/exec/bm/BmRowLayout.cpp`
- Modify: `bolt/exec/bm/BmRowContainer.h`
- Modify: `bolt/exec/bm/BmRowContainer.cpp`
- Modify: `bolt/exec/bm/BmRowContainerBatch.cpp`
- Create: `bolt/exec/bm/BmRowContainerJoin.cpp`
- Create: `bolt/exec/bm/tests/BmRowContainerJoinTest.cpp`
- Modify: `bolt/exec/bm/CMakeLists.txt`
- Modify: `bolt/exec/bm/tests/CMakeLists.txt`

**Interfaces:**
- Produces: `BmJoinLayoutOptions { uint32_t numKeys; bool hasNext; bool hasProbedFlag; bool hasNormalizedKey; }`.
- Produces: `appendBatchSelected(input, rows, partition, outRows, stringStoreMode)`.
- Produces: `next`, `setNext`, `probed`, `setProbed`, `normalizedKey`, `setNormalizedKey`, and `resetJoinRuntimeMetadata`.
- Consumed by: `BmHashTable` and BM reload.

- [ ] **Step 1: Write failing metadata layout tests**

  Add tests named `JoinLayoutKeepsUserCellsAndRuntimeMetadataDisjoint`, `JoinMetadataRoundTripsAndResets`, and `JoinMetadataDefaultsAreZero`. Exercise duplicate links, probed flag, and normalized key on two rows and assert resetting one row does not alter user cells or the other row.

- [ ] **Step 2: Write failing selected append tests**

  Add `AppendBatchSelectedPreservesSelectionOrder` using selection `{1, 3, 4}` from a mixed `BIGINT/VARCHAR` batch and `AppendBatchSelectedPreservesNullsAndLongDecimal` using nullable `HUGEINT`; assert returned rows are exactly the selected values in source order.

- [ ] **Step 3: Verify RED**

  Run `cmake --build --preset conan-release --target bolt_exec_bm_test`. Expected: compile fails because the join options and methods are absent.

- [ ] **Step 4: Implement the layout and selected writer**

  Extend the row stride after persisted cells with aligned runtime fields. Initialize runtime bytes for every appended row/range. Make selected append reserve ranges only for selected source rows and use existing typed/string store helpers; no unselected row may advance counters.

- [ ] **Step 5: Verify GREEN**

  Run:

  ```bash
  cmake --build --preset conan-release --target bolt_exec_bm_test
  _build/Release/bolt/exec/bm/tests/bolt_exec_bm_test --gtest_filter='BmRowContainerJoinTest.*:BmRowContainerTest.*' --gtest_color=no
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add bolt/exec/bm bolt/exec/bm/tests
  git commit -m "feat: add BM join row layout"
  ```

### Task 3: Add BM join equality, hash, listing, and epoch lease

**Files:**
- Modify: `bolt/exec/bm/BmRowContainer.h`
- Modify: `bolt/exec/bm/BmRowContainerCompare.cpp`
- Modify: `bolt/exec/bm/BmRowContainerExtract.cpp`
- Modify: `bolt/exec/bm/BmRowContainerReadLoad.cpp`
- Modify: `bolt/exec/bm/BmRowContainerSegmentLifecycle.cpp`
- Modify: `bolt/exec/bm/BmSegmentCollection.h`
- Modify: `bolt/exec/bm/BmSegmentCollection.cpp`
- Create: `bolt/exec/bm/BmRoundLease.{h,cpp}`
- Modify: `bolt/exec/bm/tests/BmRowContainerJoinTest.cpp`

**Interfaces:**
- Produces: `bool equalsDecoded(const char* row, int32_t column, const DecodedVector& decoded, vector_size_t index, bool nullsEqual) const`.
- Produces: `void hashRows(folly::Range<char* const*> rows, folly::Range<const int32_t*> keyColumns, raw_vector<uint64_t>& hashes) const`, using `BaseVector::hashValueAt`-compatible semantics for every supported scalar type and the same multi-key mix function as `VectorHasher`.
- Produces: `sealActivePartitionSegment`, `spillSealedPartition`, `loadPartitionRows`, and move-only `BmRoundLease`.
- Produces: lease-aware release checks shared by segment/chunk/pop/read-session release paths.

- [ ] **Step 1: Write failing equality/hash parity tests**

  Build identical rows in legacy `RowContainer` and BM storage for integer, floating point, DATE, TIMESTAMP, short/long decimal, VARCHAR, VARBINARY, null, and composite `(INTEGER, VARCHAR)`. Assert BM row-vs-decoded equality and stored-row hashes match `VectorHasher::hash()` for selected rows.

- [ ] **Step 2: Write failing spill/reload and lease tests**

  Add `SealKeepsPointersResident`, `SpillAndReloadCreatesFreshEpoch`, `SecondSpillAndReloadResetsRuntimeMetadata`, and `RoundLeaseRejectsEveryReleasePrimitive`. During a live lease, assert segment release, chunk release, pop-front, read-session release/evict, and partition spill fail while handles and row values remain valid.

- [ ] **Step 3: Verify RED**

  Run the filtered BM join tests. Expected: compile failure for missing APIs.

- [ ] **Step 4: Implement type parity and lifecycle split**

  Implement equality/hash with the same null/hash mixing rules used by decoded vectors. Split current finalize-and-flush into seal and flush. Reload all partition segments with `BatchPin`, rebase strings, mark row blocks dirty before resetting inline runtime metadata, then return the authoritative new row pointer vector.

- [ ] **Step 5: Implement the logical lease gate**

  `BmRoundLease` records partition and generation but owns no `BufferHandle`. Every primitive that can clear a covered handle or delete segment/chunk metadata checks the container lease state before mutation. Releasing the lease first invalidates the authoritative pointer view, then increments the partition generation.

- [ ] **Step 6: Verify GREEN**

  Run the full `bolt_exec_bm_test`; expected all old and new tests pass.

- [ ] **Step 7: Commit**

  ```bash
  git add bolt/exec/bm
  git commit -m "feat: add BM join epoch lifecycle"
  ```

### Task 4: Make build append and probe extraction storage-neutral

**Files:**
- Modify: `bolt/exec/HashTable.h`
- Modify: `bolt/exec/HashTable.cpp`
- Modify: `bolt/exec/HashBuild.cpp`
- Modify: `bolt/exec/HashProbe.h`
- Modify: `bolt/exec/HashProbe.cpp`
- Modify: `bolt/exec/tests/HashJoinTest.cpp`

**Interfaces:**
- Produces on `BaseHashTable`:

  ```cpp
  virtual void appendJoinRows(
      const SelectivityVector& rows,
      folly::Range<const DecodedVector* const*> keyDecoders,
      folly::Range<const DecodedVector* const*> dependentDecoders) = 0;
  virtual void extractJoinColumn(
      const char* const* rows,
      int32_t numRows,
      int32_t column,
      const VectorPtr& result) const = 0;
  virtual void setJoinProbedFlags(char* const* rows, int32_t numRows) = 0;
  virtual void extractJoinProbedFlags(
      const char* const* rows,
      int32_t numRows,
      const VectorPtr& result) const = 0;
  virtual uint64_t joinRowCount() const = 0;
  ```

- Produces: `HashProbe::resetRoundPointerState()` clearing `lookup_->hits`, `results_`, `outputTableRows_`, and last-probe iterator before table release.
- Consumed by: legacy and BM table backends.

- [ ] **Step 1: Add a legacy behavior characterization test**

  Add a test with duplicate inner-join keys and output batches smaller than the duplicate count. It must assert exact multiset results before the refactor. Add a test hook that verifies `resetRoundPointerState()` clears every build-row pointer cache.

- [ ] **Step 2: Run RED for the pointer-reset hook**

  Build and run the two filtered tests. Expected: the new reset hook test fails because cached vectors are not all cleared.

- [ ] **Step 3: Add the BaseHashTable batch contract**

  Move the legacy `newRow/store`, extract-column, probed-set, and probed-extract operations behind the new virtual methods. Do not change legacy row layout, bucket algorithms, JIT, or hash-mode selection.

- [ ] **Step 4: Centralize round pointer invalidation**

  Call `resetRoundPointerState()` before `table_.reset()` in intermediate and final-round paths and from `close()`. Make it idempotent.

- [ ] **Step 5: Verify GREEN and full legacy regression**

  Run `bolt_exec_hash_join_test`; expected 338 pass, the four baseline skips remain, and no new failures occur.

- [ ] **Step 6: Commit**

  ```bash
  git add bolt/exec/HashTable.h bolt/exec/HashTable.cpp bolt/exec/HashBuild.cpp bolt/exec/HashProbe.h bolt/exec/HashProbe.cpp bolt/exec/tests/HashJoinTest.cpp
  git commit -m "refactor: abstract hash join row storage"
  ```

### Task 5: Implement resident BmHashTable

**Files:**
- Create: `bolt/exec/BmHashTable.h`
- Create: `bolt/exec/BmHashTable.cpp`
- Create: `bolt/exec/tests/BmHashTableTest.cpp`
- Modify: `bolt/exec/CMakeLists.txt`
- Modify: `bolt/exec/tests/CMakeLists.txt`

**Interfaces:**
- Produces: `BmHashTable<true>::createForJoin(...)` for ignore-null-key inner joins.
- Produces: generic hash lookup keyed by the full 64-bit `VectorHasher` hash; each hash collision is resolved by `equalsDecoded`, and equal build keys use the row-inline `next` chain.
- Produces: runtime stats accessors for BM rows, spill bytes, spill segments, and restore count.

- [ ] **Step 1: Write failing resident table tests**

  Add tests `GenericHashFindsEverySupportedScalarType`, `HashCollisionStillChecksKeys`, `DuplicateKeysSpanOutputBatches`, `CompositeKeyAndNullsMatchLegacy`, and `RehashPreservesHits`. Compare the BM result multiset with legacy `HashTable<true>` for the same build/probe vectors.

- [ ] **Step 2: Verify RED**

  Build `bolt_exec_hash_join_test`. Expected: compile failure because `BmHashTable` does not exist.

- [ ] **Step 3: Implement the minimal generic backend**

  Use a bucket directory mapping each full hash to one or more distinct-key heads. Build hashes come from the same decoded `VectorHasher::hash()` values used for probe. On duplicate equality, prepend to the existing `next` chain; on a hash collision with unequal keys, add another distinct-key head. Implement only APIs exercised by the capability matrix; unsupported BaseHashTable operations throw a descriptive unsupported error and are unreachable behind the gate.

- [ ] **Step 4: Verify GREEN**

  Run `BmHashTableTest.*`, then the full HashJoin target.

- [ ] **Step 5: Commit**

  ```bash
  git add bolt/exec/BmHashTable.h bolt/exec/BmHashTable.cpp bolt/exec/CMakeLists.txt bolt/exec/tests/BmHashTableTest.cpp bolt/exec/tests/CMakeLists.txt
  git commit -m "feat: add resident BM hash table"
  ```

### Task 6: Capability-gate the resident BM HashJoin

**Files:**
- Modify: `bolt/core/QueryConfig.h`
- Modify: `bolt/exec/HashBuild.h`
- Modify: `bolt/exec/HashBuild.cpp`
- Create: `bolt/exec/tests/BmHashJoinTest.cpp`
- Modify: `bolt/exec/tests/CMakeLists.txt`

**Interfaces:**
- Produces: `QueryConfig::kBmHashJoinEnabled = "bm-hash-join-enabled"` and `bmHashJoinEnabled()`.
- Produces: `HashBuild::canUseBmHashJoin()` evaluated before the first build append.
- Produces: runtime stat `bmHashJoinBackend` equal to 1 only after the first BM row commits.

- [ ] **Step 1: Write failing backend-selection tests**

  Add parameterized tests for flag off, inner serial supported types, left join, parallel/multiple builders, hybrid, reusable table, JIT row equality, normalized/array mode request, and complex key/payload. Assert only the supported case reports `bmHashJoinBackend=1`; all others return correct legacy results and report 0/absence.

- [ ] **Step 2: Write failing resident operator test**

  Run a serial inner join with duplicate integer/string composite keys and a small preferred output batch. Assert result equality with DuckDB/reference and `bmHashJoinBackend=1`, `bmHashJoinSpilledBytes=0`, `bmHashJoinRestoreCount=0`.

- [ ] **Step 3: Verify RED**

  Build and run `BmHashJoinTest.*`. Expected: feature stat is absent and supported case uses legacy.

- [ ] **Step 4: Implement capability and backend factory**

  Fetch the Task-level BufferManager, create `BmHashTable<true>` only when every global constraint holds, and use `BaseHashTable::appendJoinRows`. Force `HashMode::kHash`; do not create a build `Spiller` for the BM path. Record fallback reason as a runtime string/code stat for tests and diagnostics.

- [ ] **Step 5: Verify GREEN and legacy off-path**

  Run the BM operator tests plus full HashJoin suite with the flag both absent and false.

- [ ] **Step 6: Commit**

  ```bash
  git add bolt/core/QueryConfig.h bolt/exec/HashBuild.h bolt/exec/HashBuild.cpp bolt/exec/tests/BmHashJoinTest.cpp bolt/exec/tests/CMakeLists.txt
  git commit -m "feat: integrate resident BM hash join"
  ```

### Task 7: Add real BM build spill and reload

**Files:**
- Create: `bolt/exec/BmHashJoinStorage.{h,cpp}`
- Modify: `bolt/exec/BmHashTable.{h,cpp}`
- Modify: `bolt/exec/HashBuild.{h,cpp}`
- Modify: `bolt/exec/HashJoinBridge.{h,cpp}`
- Modify: `bolt/exec/tests/BmHashJoinTest.cpp`
- Modify: `bolt/exec/tests/HashJoinBridgeTest.cpp`

**Interfaces:**
- Produces: join-lifetime `BmHashJoinStorage` owning the BmRowContainer and BufferManager.
- Produces: `BmRestoreToken { shared_ptr<BmHashJoinStorage> storage; SpillPartitionId logicalId; uint64_t generation; }`.
- Produces metrics: `bmHashJoinSpilledRows`, `bmHashJoinSpilledBytes`, `bmHashJoinSpilledSegments`, and `bmHashJoinRestoreCount`.
- Produces: Bridge table release by move-under-lock and destruction outside the bridge mutex.

- [ ] **Step 1: Write failing forced-spill tests**

  Add one case with a tiny BM join spill threshold and one with a larger threshold. Assert both match legacy results; the tiny threshold writes more segments/bytes than the larger threshold for the same deterministic data; both have positive BM spill writes and reads; legacy HashBuild row-based spill stats remain zero for the BM build path.

- [ ] **Step 2: Write failing epoch/ownership tests**

  Assert a restored table contains no pre-spill row address, all duplicate links point into the restored epoch, stale/replayed tokens fail, table destruction occurs outside the bridge mutex, and storage outlives every round guard/BufferHandle.

- [ ] **Step 3: Verify RED**

  Run the filtered BM HashJoin and bridge tests. Expected: spill metrics are zero and token APIs are missing.

- [ ] **Step 4: Implement single-logical-partition segment spill**

  At the configured threshold, seal and flush the active BM segment; continue appending into a new segment. At `noMoreInput`, seal the remaining segment, use an admission guard to load every segment, reset metadata, acquire one RoundLease, build the generic table, and publish. If admission cannot hold all rows plus the table, throw a descriptive memory error; do not replay into legacy.

- [ ] **Step 5: Implement ownership and stats**

  Make the published table retain storage and the lease. Move current table references out of the bridge lock before destruction. Forward actual BufferManager logical/physical read/write deltas into named HashBuild runtime stats.

- [ ] **Step 6: Verify GREEN**

  Run filtered spill cases, full BM tests, and full HashJoin tests. Repeat forced spill twice to catch stale generation/pointer reuse.

- [ ] **Step 7: Commit**

  ```bash
  git add bolt/exec/BmHashJoinStorage.h bolt/exec/BmHashJoinStorage.cpp bolt/exec/BmHashTable.h bolt/exec/BmHashTable.cpp bolt/exec/HashBuild.h bolt/exec/HashBuild.cpp bolt/exec/HashJoinBridge.h bolt/exec/HashJoinBridge.cpp bolt/exec/tests
  git commit -m "feat: spill and restore BM hash join rows"
  ```

### Task 8: Map the Gluten feature gate and serialize iterator access

**Files (Gluten repository):**
- Modify: `backends-bolt/src/main/scala/org/apache/gluten/config/BoltConfig.scala`
- Modify: `cpp/bolt/config/BoltConfig.h`
- Modify: `cpp/bolt/compute/WholeStageResultIterator.h`
- Modify: `cpp/bolt/compute/WholeStageResultIterator.cc`
- Modify: `cpp/core/jni/JniWrapper.cc`
- Modify: `cpp/core/utils/ObjectStore.h`
- Modify: `cpp/core/utils/ObjectStore.cc`
- Create: `cpp/bolt/compute/IteratorLifetimeGate.{h,cc}`
- Create or modify focused native tests under `cpp/bolt/tests` and `cpp/core/tests`.

**Interfaces:**
- Produces Spark key `spark.gluten.sql.columnar.backend.bolt.bmHashJoin.enabled`, default false.
- Maps it to native `bm-hash-join-enabled=true` and `buffer-manager-enabled=true`.
- Produces `IteratorLifetimeGate` with one ordinary-call token, reentrant execution guard, and closing/cancel/drain protocol.
- Produces `ObjectStore::takeForClose<T>()` atomic remove-and-return ownership.

- [ ] **Step 1: Write failing config mapping tests**

  Assert default false, explicit true maps both native QueryConfig keys, and parallel-enabled configuration leaves the Bolt capability check false.

- [ ] **Step 2: Write failing lifetime tests**

  Cover next-vs-spill overlap rejection, close-vs-blocked-next, duplicate close idempotence, take-vs-retrieve race, cancel before drain, no execution guard held while waiting on a future, and exactly-once last-owner destruction.

- [ ] **Step 3: Verify RED**

  Build the focused native tests. Expected: missing config and lifetime APIs.

- [ ] **Step 4: Implement mapping and gate**

  Ordinary JNI calls hold the exclusive active token for the full call and the execution guard only around Task/operator steps. Blocking waits release the execution guard but keep the active token. Close atomically takes ownership, enters closing, briefly acquires the guard to request cancellation, drains without the guard, then reacquires it for final teardown.

- [ ] **Step 5: Verify GREEN**

  Build `bolt_backend` and run the focused native tests. Run Scala config tests with the Bolt/Spark 3.5 profiles.

- [ ] **Step 6: Commit in Gluten**

  ```bash
  git add backends-bolt/src/main/scala/org/apache/gluten/config/BoltConfig.scala cpp/bolt cpp/core
  git commit -m "feat(bolt): gate BM hash join execution"
  ```

### Task 9: Add Gluten Spark E2E coverage

**Files (Gluten repository):**
- Modify: `backends-bolt/src/test/scala/org/apache/gluten/execution/BoltHashJoinSuite.scala`
- Modify: Bolt metrics mapping files only if named BM runtime stats are not already transported generically.

**Interfaces:**
- Consumes: BM backend and metrics from Tasks 6-8.
- Produces: deterministic Spark E2E tests comparing native output with vanilla Spark and asserting the physical `ShuffledHashJoinExecTransformer`.

- [ ] **Step 1: Add the non-spill scalar matrix**

  Generate deterministic DataFrames covering integer widths, FLOAT/DOUBLE (`-0.0` and `0.0`), DATE/TIMESTAMP, short/long decimal, string (empty, Unicode, long, null), binary, duplicate/null keys, and composite `(INTEGER, VARCHAR)` and `(DECIMAL, DATE)` keys. Use inner joins and scalar payloads. Set spill strategy `none`; assert `bmHashJoinBackend=1` and all BM spill/restore metrics are zero.

- [ ] **Step 2: Run RED**

  Run only the new wildcard suite tests. Expected: fail until the new backend library/config is packaged and metrics appear.

- [ ] **Step 3: Add forced spill and two spill-volume cases**

  Reuse the mixed fixed/string/decimal dataset. Run with tiny and moderate BM join thresholds. Assert result equality, positive BM spill write/read and restore metrics, no legacy build row-spill metrics, and `tiny.spilledSegments > moderate.spilledSegments`; use rows/segments for the stable relative assertion and record physical bytes without asserting compression-monotonicity.

- [ ] **Step 4: Build and run GREEN**

  Build/export Bolt Release, build `bolt_backend`, then run:

  ```bash
  mvn test -Pspark-3.5 -Pbackends-bolt -pl backends-bolt -am \
    -Dtest=none -DfailIfNoTests=false -Dexec.skip \
    -DwildcardSuites=org.apache.gluten.execution.BoltHashJoinSuite
  ```

  Expected: all new E2E cases pass, use the native shuffled hash join, and show the requested spill distinctions.

- [ ] **Step 5: Commit in Gluten**

  ```bash
  git add backends-bolt/src/test/scala/org/apache/gluten/execution/BoltHashJoinSuite.scala backends-bolt/src/main/scala/org/apache/gluten/backendsapi/bolt
  git commit -m "test(bolt): cover BM hash join end to end"
  ```

### Task 10: Final verification, review, and evidence audit

**Files:**
- Modify only files required by review findings.

**Interfaces:**
- Produces: review-clean Bolt and Gluten commits, no push, and an evidence checklist against the objective.

- [ ] **Step 1: Format and build narrow targets**

  Run C++ formatting for changed Bolt/Gluten files and Scala formatting for the changed suite/config. Build `bolt_exec_bm_test`, `bolt_exec_hash_join_test`, and Gluten `bolt_backend`.

- [ ] **Step 2: Run full relevant tests from fresh binaries**

  Run the complete BM test binary, complete HashJoin test binary, focused iterator/config tests, and the BoltHashJoinSuite. Save command outputs in the SDD workspace.

- [ ] **Step 3: Run a final code review**

  Review both repository diffs for lifetime/UAF, fallback timing, error handling, spill-path proof, metrics truthfulness, and test coverage. Fix every Critical/Important finding and re-run its affected tests.

- [ ] **Step 4: Audit every explicit requirement**

  Record concrete evidence for: investigation doc used; BufferManager/BmRowContainer integration; actual BM HashJoin backend; different scalar types; non-spill; actual BM spill; two spill volumes; Gluten E2E; timely commits; no push. Verify `git status`, commit logs in both repos, and remote state without pushing.

- [ ] **Step 5: Final repository checks**

  Confirm Bolt only leaves the user-owned `READY.txt` and nested `gluten/` as expected, Gluten has no unintended files, and neither repository has uncommitted task changes.
