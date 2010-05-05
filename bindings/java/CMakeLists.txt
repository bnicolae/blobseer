# FUSE specific definitions
include_directories(${JNI_INCLUDE_DIRS})
add_definitions (-D_FILE_OFFSET_BITS=64)

# Build the native blobseer 
add_library (blobseer-java SHARED blobseer_ObjectHandler.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer-java blobseer)
