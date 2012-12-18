# Include the Ruby headers
include_directories(${RUBY_INCLUDE_PATH})

# Build the native blobseer 
add_library (blobseer-ruby SHARED blobseer-ruby.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer-ruby blobseer)

# Install libraries
install (TARGETS blobseer-ruby LIBRARY DESTINATION lib)
