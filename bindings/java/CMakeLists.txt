# JNI specific definitions
include_directories(${JNI_INCLUDE_DIRS})

# Build the JNI interface 
add_library (blobseer-java SHARED blobseer_ObjectHandler.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer-java blobseer)

# Install libraries
install (TARGETS blobseer-java LIBRARY DESTINATION lib)
