# FUSE headers
include_directories (${FUSE_INCLUDE_DIR})
add_definitions (-D_FILE_OFFSET_BITS=64)

# Executable
add_executable (blob-fuse main.cpp low_level_ops.cpp fh_map.cpp migration.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (blob-fuse blobseer ${FUSE_LIBRARIES})
