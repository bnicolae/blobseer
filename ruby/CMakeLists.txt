# Include the Ruby headers
include_directories(${RUBY_INCLUDE_PATH})
include_directories(${BLOBSEER_HOME}/lib)

add_definitions()

# Build the native blobseer 
add_library (blobseer_ruby SHARED blobseer_ruby.c)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer_ruby blobseer-c)