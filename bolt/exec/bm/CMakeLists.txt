set(BOLT_EXEC_BM_STORAGE_SOURCES
  BmRowContainer.cpp
  BmRowContainerBatch.cpp
  BmBulkReadSession.cpp
  BmRowBlockLoader.cpp
  BmRowContainerCompare.cpp
  BmRowContainerExtract.cpp
  BmRowContainerReadLoad.cpp
  BmRowCopier.cpp
  BmRowLayout.cpp
  BmSegmentCollection.cpp
  BmSegmentCollectionBatch.cpp
  BmSegmentCollectionRead.cpp
  BmSegmentCollectionRelease.cpp
  BmSegmentCollectionWrite.cpp
  BmMergeReadSession.cpp
  BmReadOnlyWindowReadSession.cpp
  BmRowContainerReorder.cpp
  BmRowContainerStore.cpp
  BmRowContainerSegmentLifecycle.cpp
)

bolt_add_library(
  bolt_exec_bm_storage_object
  ${BOLT_EXEC_BM_STORAGE_SOURCES}
)

target_compile_features(bolt_exec_bm_storage_object PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm_storage_object
  bolt_memory_bm
  bolt_vector
  bolt_common_base
)

add_library(
  bolt_exec_bm_storage_support
  STATIC
  $<TARGET_OBJECTS:bolt_buffer>
  $<TARGET_OBJECTS:bolt_common_base>
  $<TARGET_OBJECTS:bolt_common_compression>
  $<TARGET_OBJECTS:bolt_common_config>
  $<TARGET_OBJECTS:bolt_encode>
  $<TARGET_OBJECTS:bolt_exception>
  $<TARGET_OBJECTS:bolt_flag_definitions>
  $<TARGET_OBJECTS:bolt_memory>
  $<TARGET_OBJECTS:bolt_process>
  $<TARGET_OBJECTS:bolt_serialization>
  $<TARGET_OBJECTS:bolt_status>
  $<TARGET_OBJECTS:bolt_time>
  $<TARGET_OBJECTS:bolt_token_extractor>
  $<TARGET_OBJECTS:bolt_type_base>
  $<TARGET_OBJECTS:bolt_type_tz>
  $<TARGET_OBJECTS:bolt_vector>
)

target_compile_features(bolt_exec_bm_storage_support PUBLIC cxx_std_20)

target_link_libraries(
  bolt_exec_bm_storage_support
  PUBLIC bolt_buffer
         bolt_common_base
         bolt_common_compression
         bolt_common_config
         bolt_encode
         bolt_exception
         bolt_flag_definitions
         bolt_memory
         bolt_process
         bolt_serialization
         bolt_status
         bolt_time
         bolt_token_extractor
         bolt_type_base
         bolt_type_tz
         bolt_vector
)

add_library(
  bolt_exec_bm_storage
  STATIC
  $<TARGET_OBJECTS:bolt_exec_bm_storage_object>
)

target_compile_features(bolt_exec_bm_storage PUBLIC cxx_std_20)

target_link_libraries(
  bolt_exec_bm_storage
  PUBLIC bolt_memory_bm
         bolt_exec_bm_storage_support
)

add_library(bolt_exec_bm INTERFACE)

target_link_libraries(
  bolt_exec_bm
  INTERFACE bolt_exec_bm_storage
            bolt_exec
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
