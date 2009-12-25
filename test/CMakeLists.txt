# Build following targets: create_blob, test, multiple_readers, multiple_writers
# Comment as needed to reduce build time
add_executable (create_blob create_blob.cpp)
add_executable (test test.cpp)
add_executable (appender appender.cpp)
add_executable (worker_mapred worker_mapred.cpp)
add_executable (reader reader.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (create_blob blobseer)
target_link_libraries (test blobseer)
target_link_libraries (appender blobseer)
target_link_libraries (worker_mapred blobseer)
target_link_libraries (reader blobseer)
