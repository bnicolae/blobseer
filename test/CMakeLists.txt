# Build following targets: create_blob, test, multiple_readers, multiple_writers
# Comment as needed to reduce build time
add_executable (create_blob create_blob.cpp)
add_executable (test test.cpp)
add_executable (file_uploader file_uploader.cpp)
add_executable (clone_test clone_test.cpp)
add_executable (fuse_test fuse_test.cpp)
#add_executable (appender appender.cpp)
#add_executable (basic_test basic_test.cpp)
#add_executable (reader reader.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (create_blob blobseer)
target_link_libraries (test blobseer)
target_link_libraries (file_uploader blobseer)
target_link_libraries (clone_test blobseer)
target_link_libraries (fuse_test blobseer)
#target_link_libraries (appender blobseer)
#target_link_libraries (basic_test blobseer)
#target_link_libraries (reader blobseer)
