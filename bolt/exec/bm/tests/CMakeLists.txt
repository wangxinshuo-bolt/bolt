include(GoogleTest)

add_executable(
  bolt_exec_bm_storage_link_test
  BmStorageLinkTest.cpp
  BmTestMain.cpp
)

target_compile_features(bolt_exec_bm_storage_link_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm_storage_link_test
  PRIVATE bolt_exec_bm_storage
          bolt_memory_bm
          GTest::gtest
          pthread
)

gtest_add_tests(bolt_exec_bm_storage_link_test "" AUTO)

target_sources(
  bolt_exec_bm_storage_link_test
  PRIVATE $<TARGET_OBJECTS:bolt_type_base>
          $<TARGET_OBJECTS:bolt_status>
          $<TARGET_OBJECTS:bolt_type_tz>
          $<TARGET_OBJECTS:bolt_serialization>
          $<TARGET_OBJECTS:bolt_encode>
          $<TARGET_OBJECTS:bolt_exception>
          $<TARGET_OBJECTS:bolt_flag_definitions>
          $<TARGET_OBJECTS:bolt_process>
)

add_executable(
  bolt_exec_bm_test
  ../benchmarks/BmRowContainerBenchmarkProfiles.cpp
  BmRowContainerBenchmarkProfileTest.cpp
  BmMergeReadSessionTest.cpp
  BmPartitionTest.cpp
  BmRowContainerBatchTest.cpp
  BmRowContainerReadTest.cpp
  BmRowContainerResidentTest.cpp
  BmSegmentCollectionTest.cpp
  BmTestMain.cpp
)

target_compile_features(bolt_exec_bm_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm_test
  PRIVATE bolt_exec_bm
          bolt_testutils
          GTest::gmock
          GTest::gtest
          pthread
)

gtest_add_tests(bolt_exec_bm_test "" AUTO)
