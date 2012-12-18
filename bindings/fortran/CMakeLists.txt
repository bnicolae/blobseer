add_library (blobseer-f SHARED blobseer-f.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer-f blobseer)

# Install libraries
install (TARGETS blobseer-f LIBRARY DESTINATION lib)
