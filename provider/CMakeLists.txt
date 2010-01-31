add_executable (provider provider.cpp pmgr_listener.cpp bdb_bw_map.cpp null_bw_map.cpp)
add_executable (sdht sdht.cpp bdb_bw_map.cpp null_bw_map.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (provider ${LIB_DEPENDENCIES} ${BDB_LIBRARIES} ${LZO_LIBRARIES})
target_link_libraries (sdht ${LIB_DEPENDENCIES} ${BDB_LIBRARIES} ${LZO_LIBRARIES})
