# Build following targets: create_blob, test, multiple_readers, multiple_writers
# Comment as needed to reduce build time
add_executable (create_blob create_blob.cpp)
add_executable (test test.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (create_blob ${LIB_DEPENDENCIES} ${CRYPTO_LIBRARIES} blobseer)
target_link_libraries (test ${LIB_DEPENDENCIES} ${CRYPTO_LIBRARIES} blobseer)
