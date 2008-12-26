include_directories (${BLOBSEER_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_executable (provider provider.cpp page_manager.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (provider ${LIB_DEPENDENCIES})
