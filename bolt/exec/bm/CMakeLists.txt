add_library(
  bolt_exec_bm_storage
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

target_compile_features(bolt_exec_bm_storage PUBLIC cxx_std_20)

target_link_libraries(
  bolt_exec_bm_storage
  PUBLIC bolt_memory_bm
         bolt_vector
         bolt_common_base
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
