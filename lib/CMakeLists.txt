include_directories (${BLOBSEER_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_library (blobseer SHARED blobseer.cpp ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)

# Link the executable to the Hello library.
target_link_libraries (blobseer ${LIB_DEPENDENCIES} ${OPENSSL_LIBRARIES})
