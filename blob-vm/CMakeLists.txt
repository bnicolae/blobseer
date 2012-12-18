# Check for FUSE
set(FUSE_ROOT ${EXTERNAL_ROOT})
find_package(FUSE)

if (FUSE_FOUND)

  message(STATUS "Building FUSE support. FUSE headers: ${FUSE_INCLUDE_DIR}")
  
  include_directories (${FUSE_INCLUDE_DIR})
  add_definitions (-D_FILE_OFFSET_BITS=64)

  add_executable (blob-vm main.cpp low_level_ops.cpp fh_map.cpp migration.cpp)
  target_link_libraries (blob-vm blobseer ${FUSE_LIBRARIES})

  install (TARGETS blob-vm RUNTIME DESTINATION bin)
  install (FILES blob_ioctl.hpp DESTINATION include)

else (FUSE_FOUND)

  message(STATUS "FUSE headers not found. FUSE support will not be built.")

endif (FUSE_FOUND)

