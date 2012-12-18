#  The following CMake variables will be defined:
# 
#  FUSE_FOUND - system has the FUSE library
#  FUSE_INCLUDE_DIR - the FUSE include directory
#  FUSE_LIBRARIES - The libraries needed to use FUSE

if (FUSE_INCLUDE_DIR AND FUSE_LIBRARIES)
  # in cache already
  SET(FUSE_FOUND TRUE)

else (FUSE_INCLUDE_DIR AND FUSE_LIBRARIES)
  FIND_PATH(FUSE_INCLUDE_DIR fuse_lowlevel.h
     ${FUSE_ROOT}/include/fuse/
     /usr/include/fuse/
     /usr/local/include/fuse/
     /sw/include/fuse/
     /sw/local/include/fuse/
  )

  if(WIN32 AND MSVC)
  else(WIN32 AND MSVC)
    FIND_LIBRARY(FUSE_LIBRARIES NAMES fuse
      PATHS
      ${FUSE_ROOT}/lib
      /usr/lib
      /usr/local/lib
      /sw/lib
      /sw/local/lib
    )
  endif(WIN32 AND MSVC)

  if (FUSE_INCLUDE_DIR AND FUSE_LIBRARIES)
     set(FUSE_FOUND TRUE)
  endif (FUSE_INCLUDE_DIR AND FUSE_LIBRARIES)

  if (FUSE_FOUND)
     if (NOT FUSE_FIND_QUIETLY)
        message(STATUS "Found FUSE: ${FUSE_LIBRARIES}")
     endif (NOT FUSE_FIND_QUIETLY)
  else (FUSE_FOUND)
     if (FUSE_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find FUSE")
     endif (FUSE_FIND_REQUIRED)
  endif (FUSE_FOUND)

  MARK_AS_ADVANCED(FUSE_INCLUDE_DIR FUSE_LIBRARIES)

endif (FUSE_INCLUDE_DIR AND FUSE_LIBRARIES)