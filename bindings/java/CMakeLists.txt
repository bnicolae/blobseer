# Include the JNI headers
include_directories(${JNI_INCLUDE_DIRS})

# Build the native blobseer 
add_library (blobseer-java SHARED blobseer_ObjectHandler.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer-java blobseer)
