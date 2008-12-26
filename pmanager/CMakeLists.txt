include_directories (${BLOBSEER_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_executable (pmanager adv_manager.cpp publisher.cpp)

# Link the executable to the Hello library.
target_link_libraries (pmanager ${LIB_DEPENDENCIES})
