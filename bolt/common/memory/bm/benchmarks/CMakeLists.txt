# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_executable(
  bolt_buffer_manager_sort_benchmark
  BufferManagerSortBenchmark.cpp)

target_compile_features(bolt_buffer_manager_sort_benchmark PRIVATE cxx_std_20)

target_link_libraries(
  bolt_buffer_manager_sort_benchmark
  PRIVATE bolt_memory
          bolt_common_base
          bolt_common_compression
          bolt_common_config
          bolt_dwio_common_exception
          bolt_encode
          bolt_exception
          bolt_flag_definitions
          bolt_process
          bolt_serialization
          bolt_status
          bolt_time
          bolt_type_base
          bolt_type_tz
          bolt_token_extractor
          fmt::fmt
          gflags::gflags
          glog::glog
          pthread
          dl)
