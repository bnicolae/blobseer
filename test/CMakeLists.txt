add_executable (create_blob create_blob.cpp ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)
add_executable (test test.cpp ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (create_blob ${LIB_DEPENDENCIES} ${OPENSSL_LIBRARIES})
target_link_libraries (test ${LIB_DEPENDENCIES} ${OPENSSL_LIBRARIES})
