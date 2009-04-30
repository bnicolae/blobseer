add_executable (provider provider.cpp pmgr_listener.cpp 
  ${BLOBSEER_SOURCE_DIR}/common/bdb_bw_map.cpp ${BLOBSEER_SOURCE_DIR}/common/null_bw_map.cpp)
add_executable (sdht sdht.cpp ${BLOBSEER_SOURCE_DIR}/common/bdb_bw_map.cpp ${BLOBSEER_SOURCE_DIR}/common/null_bw_map.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (provider ${LIB_DEPENDENCIES} ${BDB_LIBRARIES})
target_link_libraries (sdht ${LIB_DEPENDENCIES} ${BDB_LIBRARIES})
