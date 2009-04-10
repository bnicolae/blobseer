add_executable (provider provider.cpp page_manager.cpp pmgr_listener.cpp ${BLOBSEER_SOURCE_DIR}/common/buffer_wrapper_map.cpp)
add_executable (sdht sdht.cpp page_manager.cpp ${BLOBSEER_SOURCE_DIR}/common/buffer_wrapper_map.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (provider ${LIB_DEPENDENCIES} ${BDB_LIBRARIES})
target_link_libraries (sdht ${LIB_DEPENDENCIES} ${BDB_LIBRARIES})
