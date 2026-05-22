# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

include(GoogleTest)

add_executable(
  bolt_memory_bm_test
  BlockHandleTest.cpp
  BufferPoolTest.cpp
  DiskIoTest.cpp
  DiskProbeTest.cpp
  MemoryManagerIntegrationTest.cpp
  SpillTest.cpp
  SpillFileStoreTest.cpp)

target_compile_features(bolt_memory_bm_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_test
  PRIVATE bolt_testutils GTest::gtest GTest::gtest_main pthread dl)

gtest_add_tests(bolt_memory_bm_test "" AUTO)
