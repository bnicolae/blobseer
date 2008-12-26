include_directories (${BLOBSEER_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_executable (provider provider.cpp page_manager.cpp)

# Link the executable to the Hello library.
target_link_libraries (provider ${LIB_DEPENDENCIES})
