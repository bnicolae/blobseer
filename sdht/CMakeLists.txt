include_directories (${BLOBSEER_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_executable (sdht sdht.cpp page_manager.cpp)

# Link the executable to the Hello library.
target_link_libraries (sdht ${LIB_DEPENDENCIES})
