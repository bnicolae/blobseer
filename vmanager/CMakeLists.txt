include_directories (${BLOBSEER_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_executable (vmanager main.cpp vmanagement.cpp)

# Link the executable to the Hello library.
target_link_libraries (vmanager ${LIB_DEPENDENCIES})
