# Build following targets: create_blob, test, multiple_readers, multiple_writers
# Comment as needed to reduce build time
add_executable (create_blob create_blob.cpp ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)
add_executable (test test.cpp ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)
#add_executable (multiple_readers multiple_readers.cpp ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)
#add_executable (multiple_writers multiple_writers.cpp ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (create_blob ${LIB_DEPENDENCIES} ${CRYPTO_LIBRARIES})
target_link_libraries (test ${LIB_DEPENDENCIES} ${CRYPTO_LIBRARIES})
#target_link_libraries (multiple_readers ${LIB_DEPENDENCIES} ${CRYPTO_LIBRARIES})
#target_link_libraries (multiple_writers ${LIB_DEPENDENCIES} ${CRYPTO_LIBRARIES})
