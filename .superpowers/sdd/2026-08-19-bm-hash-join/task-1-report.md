# Task 1 Report: Break the BM storage link cycle

## What changed

This change split BM row storage into a standalone lower-level library target,
`bolt_exec_bm_storage`, so later code can link BM storage without forcing a
`bolt_exec` dependency cycle.

It also kept the existing `bolt_exec_bm` name as a compatibility interface
target for current BM tests and benchmarks, and added a dedicated smoke test
that links BM storage without `bolt_exec`.

## Files changed

- `bolt/exec/CMakeLists.txt`
- `bolt/exec/bm/CMakeLists.txt`
- `bolt/exec/bm/tests/CMakeLists.txt`
- `bolt/exec/bm/tests/BmStorageLinkTest.cpp`

## RED

### Command

```bash
make release_with_test BOLT_CONAN_CONFIGURE_ONLY=1
cmake --build --preset conan-release --target bolt_exec_bm_storage_link_test
```

### Output

```text
[1/3] Building CXX object bolt/exec/bm/tests/CMakeFiles/bolt_exec_bm_storage_link_test.dir/BmStorageLinkTest.cpp.o
[2/3] Building CXX object bolt/exec/bm/tests/CMakeFiles/bolt_exec_bm_storage_link_test.dir/BmTestMain.cpp.o
[3/3] Linking CXX executable bolt/exec/bm/tests/bolt_exec_bm_storage_link_test
FAILED: bolt/exec/bm/tests/bolt_exec_bm_storage_link_test
...
/usr/bin/ld: cannot find -lbolt_exec_bm_storage
collect2: error: ld returned 1 exit status
ninja: build stopped: subcommand failed.
```

### Why this RED was expected

The new smoke target referenced `bolt_exec_bm_storage` before that target
existed anywhere in the build graph. The failure therefore proved the test was
covering the intended missing target split rather than some unrelated compile
error.

## GREEN

### Build command

```bash
cmake --build --preset conan-release --target bolt_exec_bm_storage_link_test bolt_exec_bm_test bolt_exec_hash_join_test
```

### Build result

```text
ninja: no work to do.
```

This was the final rerun after the fixes were already built successfully in the
same configured tree, so Ninja correctly reported the target set as up to date.

### Smoke test command

```bash
_build/Release/bolt/exec/bm/tests/bolt_exec_bm_storage_link_test --gtest_color=no
```

### Smoke test result

```text
[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from BmStorageLinkTest
[ RUN      ] BmStorageLinkTest.ComputesRowSizeWithoutBoltExec
[       OK ] BmStorageLinkTest.ComputesRowSizeWithoutBoltExec (0 ms)
[----------] 1 test from BmStorageLinkTest (0 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (0 ms total)
[  PASSED  ] 1 test.
```

### BM regression test command

```bash
_build/Release/bolt/exec/bm/tests/bolt_exec_bm_test --gtest_color=no
```

### BM regression test result

```text
[==========] Running 44 tests from 2 test suites.
...
[==========] 44 tests from 2 test suites ran. (82 ms total)
[  PASSED  ] 44 tests.
```

Notes:
- `bolt_exec_hash_join_test` was validated at build time via the required
  `cmake --build ... --target ... bolt_exec_hash_join_test` command.
- `bolt_exec_bm_test` emitted expected negative-path exception logs during some
  passing tests; the overall test binary exited successfully with all 44 tests
  passing.

## Implementation details

- `bolt/exec/bm/CMakeLists.txt`
  - Replaced the old BM storage object target with a concrete static library
    named `bolt_exec_bm_storage`.
  - Linked `bolt_exec_bm_storage` to `bolt_memory_bm`, `bolt_vector`, and
    `bolt_common_base`.
  - Added compatibility interface target `bolt_exec_bm` that links
    `bolt_exec_bm_storage` and `bolt_exec`.

- `bolt/exec/CMakeLists.txt`
  - Linked `bolt_exec` directly against `bolt_exec_bm_storage`.

- `bolt/exec/bm/tests/CMakeLists.txt`
  - Added `bolt_exec_bm_storage_link_test`.
  - Kept it off `bolt_exec`.
  - Added explicit object-target sources needed by this repository's
    object-library build pattern so the smoke executable can link standalone.

- `bolt/exec/bm/tests/BmStorageLinkTest.cpp`
  - Added a smoke test that constructs a `BmRowLayout` for
    `{BIGINT(), VARCHAR()}` and asserts `rowSize() > 0`.

## Self-review

- The target split matches the requirement boundary: BM storage is now a
  concrete lower-level library and `bolt_exec` consumes it directly.
- Existing BM tests and benchmarks keep using `bolt_exec_bm`, so compatibility
  is preserved for current callers.
- The smoke test meaningfully exercises the intended link boundary by using
  `BmRowLayout` through `bolt_exec_bm_storage` without linking `bolt_exec`.
- One repo-specific wrinkle is that Bolt uses many object libraries, so the
  smoke test executable needed explicit `target_sources($<TARGET_OBJECTS:...>)`
  additions for lower-level type/process/flag objects. This follows existing
  patterns already used by BM memory tests in the repo.
- I did not touch `READY.txt` or `gluten/`.

## Concerns

- The standalone smoke target currently needs explicit object-target additions
  because some low-level dependencies in this build system are not fully
  expressed as ordinary archive/shared-library edges. That is acceptable for
  this task and keeps the smoke test independent of `bolt_exec`, but it also
  highlights existing build-system coupling outside the BM split itself.

## Review-fix round 1

### Review findings addressed

- High: `bolt_exec_bm_storage_link_test` must link only through
  `bolt_exec_bm_storage`, `bolt_memory_bm`, and GTest, with no direct
  `$<TARGET_OBJECTS:...>` injections on the executable.
- Medium: preserve `bolt_engine` aggregation/export/package behavior by keeping
  BM storage objects inside `BOLT_EXECUTION_OBJECT_TARGETS`.

### Implementation changes

- `bolt/exec/bm/CMakeLists.txt`
  - Introduced `BOLT_EXEC_BM_STORAGE_SOURCES`.
  - Added `bolt_exec_bm_storage_object` via `bolt_add_library(...)` so the BM
    storage compilation units continue to participate in
    `BOLT_EXECUTION_OBJECT_TARGETS`.
  - Wrapped that object target in a public static library
    `bolt_exec_bm_storage` that embeds the additional low-level object
    libraries required for standalone linking.
  - Kept compatibility target `bolt_exec_bm` as an interface that links
    `bolt_exec_bm_storage` and `bolt_exec`.
- `bolt/exec/bm/tests/CMakeLists.txt`
  - Removed the explicit `target_sources(... $<TARGET_OBJECTS:...>)` block from
    `bolt_exec_bm_storage_link_test`.
  - Kept the smoke target linked only to `bolt_exec_bm_storage`,
    `bolt_memory_bm`, `GTest::gtest`, and `pthread`.

### Reconfigure command

```bash
make release_with_test BOLT_CONAN_CONFIGURE_ONLY=1
```

### Reconfigure result

```text
-- Configuring done (4.1s)
-- Generating done (6.6s)
-- Build files have been written to: /data00/home/wangxinshuo.db/bolt/_build/Release
```

Direct aggregation evidence from the generated CMake cache output:

```text
BOLT_EXECUTION_OBJECT_TARGETS = ...;$<TARGET_OBJECTS:bolt_exec>;$<TARGET_OBJECTS:bolt_exec_bm_storage_object>
```

This confirms the BM storage object target is again part of the `bolt_engine`
object aggregation path.

### Build command

```bash
cmake --build --preset conan-release --target bolt_exec_bm_storage_link_test bolt_exec_bm_test bolt_exec_hash_join_test bolt_engine
```

### Build result

```text
[37/48] Linking CXX static library bolt/exec/bm/libbolt_exec_bm_storage.a
[38/48] Linking CXX executable bolt/exec/bm/tests/bolt_exec_bm_storage_link_test
[45/48] Linking CXX static library bolt/libbolt_engine.a
[47/48] Linking CXX executable bolt/exec/bm/tests/bolt_exec_bm_test
[48/48] Linking CXX executable bolt/exec/tests/bolt_exec_hash_join_test
```

### Smoke test command

```bash
_build/Release/bolt/exec/bm/tests/bolt_exec_bm_storage_link_test --gtest_color=no
```

### Smoke test result

```text
[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from BmStorageLinkTest
[ RUN      ] BmStorageLinkTest.ComputesRowSizeWithoutBoltExec
[       OK ] BmStorageLinkTest.ComputesRowSizeWithoutBoltExec (0 ms)
[----------] 1 test from BmStorageLinkTest (0 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (0 ms total)
[  PASSED  ] 1 test.
```

### BM regression test command

```bash
_build/Release/bolt/exec/bm/tests/bolt_exec_bm_test --gtest_color=no
```

### BM regression test result

```text
[==========] Running 44 tests from 2 test suites.
...
[==========] 44 tests from 2 test suites ran. (84 ms total)
[  PASSED  ] 44 tests.
```

Notes:
- `bolt_exec_bm_test` still emits expected exception logs for negative-path
  coverage, but the binary exits successfully with all 44 tests passing.
- `bolt_exec_hash_join_test` was verified by the required build target in the
  same `cmake --build ...` invocation.

### Link-graph and archive evidence

Evidence from the generated Ninja graph:

```text
build cmake_object_order_depends_target_bolt_engine: phony ... cmake_object_order_depends_target_bolt_exec_bm_storage_object ...
build bolt_exec_bm_storage_link_test: phony bolt/exec/bm/tests/bolt_exec_bm_storage_link_test
```

Evidence from the smoke target link line in `build.ninja`:

```text
LINK_LIBRARIES = ... bolt/libbolt_testutils.a ... bolt/exec/bm/libbolt_exec_bm_storage.a ... bolt/common/memory/bm/libbolt_memory_bm.a ...
```

Evidence from the built archives:

```text
$ ar t _build/Release/bolt/libbolt_engine.a | rg 'Bm(Row|Segment|Bulk|Merge|ReadOnly)'
BmRowContainer.cpp.o
BmRowContainerBatch.cpp.o
BmBulkReadSession.cpp.o
BmRowBlockLoader.cpp.o
BmRowContainerCompare.cpp.o
BmRowContainerExtract.cpp.o
BmRowContainerReadLoad.cpp.o
BmRowCopier.cpp.o
BmRowLayout.cpp.o
BmSegmentCollection.cpp.o
BmSegmentCollectionBatch.cpp.o
BmSegmentCollectionRead.cpp.o
BmSegmentCollectionRelease.cpp.o
BmSegmentCollectionWrite.cpp.o
BmMergeReadSession.cpp.o
BmReadOnlyWindowReadSession.cpp.o
BmRowContainerReorder.cpp.o
BmRowContainerStore.cpp.o
BmRowContainerSegmentLifecycle.cpp.o
```

```text
$ ar t _build/Release/bolt/exec/bm/libbolt_exec_bm_storage.a | sed -n '1,80p'
BmRowContainer.cpp.o
BmRowContainerBatch.cpp.o
BmBulkReadSession.cpp.o
BmRowBlockLoader.cpp.o
BmRowContainerCompare.cpp.o
BmRowContainerExtract.cpp.o
BmRowContainerReadLoad.cpp.o
BmRowCopier.cpp.o
BmRowLayout.cpp.o
BmSegmentCollection.cpp.o
BmSegmentCollectionBatch.cpp.o
BmSegmentCollectionRead.cpp.o
BmSegmentCollectionRelease.cpp.o
BmSegmentCollectionWrite.cpp.o
BmMergeReadSession.cpp.o
BmReadOnlyWindowReadSession.cpp.o
BmRowContainerReorder.cpp.o
BmRowContainerStore.cpp.o
BmRowContainerSegmentLifecycle.cpp.o
...
```

### Review-fix conclusion

- The smoke executable now links through the declared library boundary without
  direct object injections on the test target.
- `bolt_exec_bm_storage_object` is back inside
  `BOLT_EXECUTION_OBJECT_TARGETS`, and the built `bolt_engine` archive contains
  the BM storage objects again.
- This resolves both blocking review findings without reintroducing a
  `bolt_exec <-> BM storage` cycle.

## Review fix round 2

### Problem addressed

The round-1 shape still left `bolt_exec_bm_storage` embedding non-BM objects,
which violated the requested lower-level boundary. The follow-up requirement was
to keep `bolt_exec_bm_storage` BM-only, keep the smoke link target on the
declared boundary, and prove both archive and link shape after the fix.

### Implementation

- Kept `bolt_exec_bm_storage_object` as the BM-only object target that still
  feeds `BOLT_EXECUTION_OBJECT_TARGETS`.
- Added a narrow `bolt_exec_bm_storage_support` static wrapper for the lower
  dependency closure needed by BM storage when linked standalone.
- Changed `bolt_exec_bm_storage` to archive only
  `$<TARGET_OBJECTS:bolt_exec_bm_storage_object>` and link publicly to
  `bolt_memory_bm` plus `bolt_exec_bm_storage_support`.
- Kept the smoke executable linked only through
  `bolt_exec_bm_storage`, `bolt_memory_bm`, and `GTest::gtest`, with no direct
  `pthread` entry on the test target.

### Why the support wrapper was required

An attempted `INTERFACE`-only dependency shape failed to link the smoke target
with unresolved symbols including:

```text
undefined reference to bytedance::bolt::BIGINT()
undefined reference to bytedance::bolt::VARCHAR()
undefined reference to fLB::FLAGS_bolt_enable_memory_usage_track_in_default_memory_pool
undefined reference to fLB::FLAGS_bolt_memory_leak_check_enabled
undefined reference to bytedance::bolt::mapTypeKindToName[abi:cxx11](...)
undefined reference to bytedance::bolt::process::StackTrace::...
```

That proved this repo cannot currently consume the BM storage library standalone
through pure `INTERFACE` lower-level edges, so the fix uses a narrow normal
archive wrapper instead of copying non-BM objects into the BM archive itself.

### Final validation commands

```bash
make release_with_test BOLT_CONAN_CONFIGURE_ONLY=1
cmake --build --preset conan-release --target bolt_exec_bm_storage_link_test
cmake --build --preset conan-release --target bolt_exec_bm_storage_link_test bolt_exec_bm_test bolt_exec_hash_join_test bolt_engine
_build/Release/bolt/exec/bm/tests/bolt_exec_bm_storage_link_test --gtest_color=no
_build/Release/bolt/exec/bm/tests/bolt_exec_bm_test --gtest_color=no
```

### Final validation results

Configure-only rerun preserved the existing cache and regenerated successfully:

```text
Conan options and build type unchanged; preserving CMakeCache.txt
-- Configuring done (4.0s)
-- Generating done (6.2s)
-- Build files have been written to: /data00/home/wangxinshuo.db/bolt/_build/Release
```

Required build completed successfully:

```text
[13/16] Linking CXX static library bolt/libbolt_engine.a
[15/16] Linking CXX executable bolt/exec/bm/tests/bolt_exec_bm_test
[16/16] Linking CXX executable bolt/exec/tests/bolt_exec_hash_join_test
```

Smoke test passed:

```text
[==========] Running 1 test from 1 test suite.
[  PASSED  ] 1 test.
```

BM regression test passed:

```text
[==========] Running 44 tests from 2 test suites.
[  PASSED  ] 44 tests.
```

### Link and archive proof

Generated Ninja link rule for the smoke target:

```text
build bolt/exec/bm/tests/bolt_exec_bm_storage_link_test: CXX_EXECUTABLE_LINKER__bolt_exec_bm_storage_link_test_Release ... | bolt/exec/bm/libbolt_exec_bm_storage.a bolt/common/memory/bm/libbolt_memory_bm.a ... bolt/exec/bm/libbolt_exec_bm_storage_support.a ...
LINK_LIBRARIES = ... bolt/exec/bm/libbolt_exec_bm_storage.a ... bolt/common/memory/bm/libbolt_memory_bm.a ... bolt/exec/bm/libbolt_exec_bm_storage_support.a ...
```

The exact smoke target link rule contains `bolt_exec_bm_storage`,
`bolt_memory_bm`, and `bolt_exec_bm_storage_support`, and it does not contain
`bolt_exec` or `bolt_engine`.

`bolt_exec_bm_storage` archive contents are BM-only:

```text
BmRowContainer.cpp.o
BmRowContainerBatch.cpp.o
BmBulkReadSession.cpp.o
BmRowBlockLoader.cpp.o
BmRowContainerCompare.cpp.o
BmRowContainerExtract.cpp.o
BmRowContainerReadLoad.cpp.o
BmRowCopier.cpp.o
BmRowLayout.cpp.o
BmSegmentCollection.cpp.o
BmSegmentCollectionBatch.cpp.o
BmSegmentCollectionRead.cpp.o
BmSegmentCollectionRelease.cpp.o
BmSegmentCollectionWrite.cpp.o
BmMergeReadSession.cpp.o
BmReadOnlyWindowReadSession.cpp.o
BmRowContainerReorder.cpp.o
BmRowContainerStore.cpp.o
BmRowContainerSegmentLifecycle.cpp.o
```

`bolt_engine` still aggregates the BM storage objects:

```text
BmRowContainer.cpp.o
BmRowContainerBatch.cpp.o
BmBulkReadSession.cpp.o
BmRowBlockLoader.cpp.o
BmRowContainerCompare.cpp.o
BmRowContainerExtract.cpp.o
BmRowContainerReadLoad.cpp.o
BmRowCopier.cpp.o
BmRowLayout.cpp.o
BmSegmentCollection.cpp.o
BmSegmentCollectionBatch.cpp.o
BmSegmentCollectionRead.cpp.o
BmSegmentCollectionRelease.cpp.o
BmSegmentCollectionWrite.cpp.o
BmMergeReadSession.cpp.o
BmReadOnlyWindowReadSession.cpp.o
BmRowContainerReorder.cpp.o
BmRowContainerStore.cpp.o
BmRowContainerSegmentLifecycle.cpp.o
```

### Round-2 conclusion

- `bolt_exec_bm_storage` now contains only BM storage objects.
- The smoke test links through the intended boundary and does not pull in
  `bolt_exec` or `bolt_engine`.
- `bolt_engine` still aggregates BM storage objects via
  `bolt_exec_bm_storage_object`.
- The support archive is the narrow fallback needed to satisfy the current
  standalone link closure without hiding non-BM objects inside the BM archive.

## Review fix round 4

### Controller ruling applied

The standalone smoke executable and support archive were removed. The accepted
shape is now a native `bolt_add_library` OBJECT target named
`bolt_exec_bm_storage`, containing only BM storage sources, with direct target
dependencies on `bolt_memory_bm`, `bolt_vector`, and `bolt_common_base`.

### Implementation changes

- `bolt/exec/bm/CMakeLists.txt`
  - Replaced the round-2 static wrapper/support archive shape with a single
    `bolt_add_library(bolt_exec_bm_storage ...)` OBJECT target.
  - Removed the `bolt_exec_bm` compatibility target to avoid injecting BM
    storage objects beside `bolt_engine`.
  - Added configure-time `get_target_property` assertions for target `TYPE`,
    required `LINK_LIBRARIES`, and forbidden `bolt_exec`/`bolt_engine` edges.
- `bolt/exec/CMakeLists.txt`
  - Kept `bolt_exec` free of any `bolt_exec_bm_storage` link edge.
- `bolt/exec/bm/tests/CMakeLists.txt`
  - Deleted `bolt_exec_bm_storage_link_test`.
  - Linked `bolt_exec_bm_test` through `bolt_testutils`, which already brings
    `bolt_engine`.
- `bolt/exec/bm/benchmarks/CMakeLists.txt`
  - Removed the obsolete `bolt_exec_bm` link and left benchmark linkage through
    `bolt_testutils`.
- `bolt/exec/bm/tests/BmStorageLinkTest.cpp`
  - Deleted the obsolete standalone smoke test source.

### Configure command

```bash
make release_with_test BOLT_CONAN_CONFIGURE_ONLY=1
```

### Configure result

```text
Conan options and build type unchanged; preserving CMakeCache.txt
-- Configuring done (4.1s)
-- Generating done (6.1s)
-- Build files have been written to: /data00/home/wangxinshuo.db/bolt/_build/Release
```

Generated CMake variable evidence:

```text
BOLT_EXECUTION_OBJECT_TARGETS = ...;$<TARGET_OBJECTS:bolt_exec>;$<TARGET_OBJECTS:bolt_exec_bm_storage>
```

The new configure-time target assertions ran during this configure step.

### Build commands

```bash
cmake --build --preset conan-release --target bolt_exec_bm_storage bolt_engine bolt_exec_bm_test bolt_exec_hash_join_test
cmake --build --preset conan-release --target bolt_exec_bm_storage
```

### Build results

```text
[33/36] Linking CXX static library bolt/libbolt_engine.a
[34/36] Linking CXX static library bolt/libbolt_testutils.a
[35/36] Linking CXX executable bolt/exec/bm/tests/bolt_exec_bm_test
[36/36] Linking CXX executable bolt/exec/tests/bolt_exec_hash_join_test
```

The second narrow storage-target rebuild confirmed the OBJECT target is present
and up to date:

```text
ninja: no work to do.
```

### BM regression test command

```bash
_build/Release/bolt/exec/bm/tests/bolt_exec_bm_test --gtest_color=no
```

### BM regression test result

```text
[==========] Running 44 tests from 2 test suites.
[==========] 44 tests from 2 test suites ran. (72 ms total)
[  PASSED  ] 44 tests.
```

The binary still emits expected exception logs for negative-path assertions, and
exited successfully.

### Graph and archive evidence

Focused Graphviz extraction for `bolt_exec_bm_storage`:

```text
storage_node="node687"
    "node687" -> "node12"  // bolt_exec_bm_storage -> bolt_common_base
    "node687" -> "node266"  // bolt_exec_bm_storage -> bolt_memory_bm
    "node687" -> "node246"  // bolt_exec_bm_storage -> bolt_vector
```

No `bolt_exec` or `bolt_engine` edge exists from `bolt_exec_bm_storage` in the
generated target graph.

Generated Ninja object-library evidence:

```text
build bolt/exec/bm/bolt_exec_bm_storage: phony bolt/exec/bm/CMakeFiles/bolt_exec_bm_storage.dir/BmRowContainer.cpp.o ... bolt/exec/bm/CMakeFiles/bolt_exec_bm_storage.dir/BmRowContainerSegmentLifecycle.cpp.o
build bolt_exec_bm_storage: phony bolt/exec/bm/bolt_exec_bm_storage
```

`bolt_engine` archive contains every BM storage source exactly once:

```text
BmRowContainer.cpp.o 1
BmRowContainerBatch.cpp.o 1
BmBulkReadSession.cpp.o 1
BmRowBlockLoader.cpp.o 1
BmRowContainerCompare.cpp.o 1
BmRowContainerExtract.cpp.o 1
BmRowContainerReadLoad.cpp.o 1
BmRowCopier.cpp.o 1
BmRowLayout.cpp.o 1
BmSegmentCollection.cpp.o 1
BmSegmentCollectionBatch.cpp.o 1
BmSegmentCollectionRead.cpp.o 1
BmSegmentCollectionRelease.cpp.o 1
BmSegmentCollectionWrite.cpp.o 1
BmMergeReadSession.cpp.o 1
BmReadOnlyWindowReadSession.cpp.o 1
BmRowContainerReorder.cpp.o 1
BmRowContainerStore.cpp.o 1
BmRowContainerSegmentLifecycle.cpp.o 1
```

Reference scan for obsolete targets:

```text
rg -n "bolt_exec_bm\b|bolt_exec_bm_storage_link_test|BmStorageLinkTest|bolt_exec_bm_storage_support|bolt_exec_bm_storage_object" bolt -g 'CMakeLists.txt' -g '*.cmake' -g '*.cpp'
```

Result: no matches.

### Self-review

- The final shape follows the latest controller ruling: no wrapper static
  library, no support archive, no copied non-BM object lists, and no standalone
  smoke executable.
- `bolt_exec_bm_storage` is an OBJECT library created through
  `bolt_add_library`, so it participates in the existing `bolt_engine`
  aggregation path.
- `bolt_exec` does not link `bolt_exec_bm_storage`; `bolt_engine` aggregates
  both object targets once.
- Existing BM tests and benchmarks no longer use the removed compatibility
  target and instead rely on `bolt_testutils`/`bolt_engine`.
