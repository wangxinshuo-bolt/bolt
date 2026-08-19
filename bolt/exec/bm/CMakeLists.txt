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
  bolt_exec_bm_storage
  ${BOLT_EXEC_BM_STORAGE_SOURCES}
)

target_compile_features(bolt_exec_bm_storage PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm_storage
  bolt_memory_bm
  bolt_vector
  bolt_common_base
)

get_target_property(BOLT_EXEC_BM_STORAGE_TYPE bolt_exec_bm_storage TYPE)
if(NOT BOLT_EXEC_BM_STORAGE_TYPE STREQUAL "OBJECT_LIBRARY")
  message(FATAL_ERROR "bolt_exec_bm_storage must be an OBJECT_LIBRARY")
endif()

get_target_property(BOLT_EXEC_BM_STORAGE_LINK_LIBRARIES bolt_exec_bm_storage LINK_LIBRARIES)
foreach(BOLT_EXEC_BM_STORAGE_REQUIRED_DEP IN ITEMS bolt_memory_bm bolt_vector bolt_common_base)
  list(FIND BOLT_EXEC_BM_STORAGE_LINK_LIBRARIES ${BOLT_EXEC_BM_STORAGE_REQUIRED_DEP} BOLT_EXEC_BM_STORAGE_DEP_INDEX)
  if(BOLT_EXEC_BM_STORAGE_DEP_INDEX EQUAL -1)
    message(FATAL_ERROR "bolt_exec_bm_storage must link ${BOLT_EXEC_BM_STORAGE_REQUIRED_DEP}")
  endif()
endforeach()

foreach(BOLT_EXEC_BM_STORAGE_FORBIDDEN_DEP IN ITEMS bolt_exec bolt_engine)
  list(FIND BOLT_EXEC_BM_STORAGE_LINK_LIBRARIES ${BOLT_EXEC_BM_STORAGE_FORBIDDEN_DEP} BOLT_EXEC_BM_STORAGE_DEP_INDEX)
  if(NOT BOLT_EXEC_BM_STORAGE_DEP_INDEX EQUAL -1)
    message(FATAL_ERROR "bolt_exec_bm_storage must not link ${BOLT_EXEC_BM_STORAGE_FORBIDDEN_DEP}")
  endif()
endforeach()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
