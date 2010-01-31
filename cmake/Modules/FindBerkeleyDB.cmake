# Find libconfig
#  BDB_FOUND - system has the Berkeley DB library
#  BDB_INCLUDE_DIR - the Berkeley DB include directory
#  BDB_LIBRARIES - The libraries needed to use Berkeley DB

if (BDB_INCLUDE_DIR AND BDB_LIBRARIES)

  # in cache already
  SET(BDB_FOUND TRUE)

else (BDB_INCLUDE_DIR AND BDB_LIBRARIES)
  FIND_PATH(BDB_INCLUDE_DIR db_cxx.h
     ${BDB_ROOT}/include
#     /usr/include/
     /usr/local/include/
     /sw/lib
     /sw/local/lib
     $ENV{ProgramFiles}/BDB/include/
     $ENV{SystemDrive}/BDB/include/
  )

  if(WIN32 AND MSVC)
  else(WIN32 AND MSVC)
    FIND_LIBRARY(BDB_LIBRARIES NAMES db_cxx
      PATHS
      ${BDB_ROOT}/lib
      /sw/lib
      /sw/local/lib
#      /usr/lib
      /usr/local/lib
    )
  endif(WIN32 AND MSVC)

  if (BDB_INCLUDE_DIR AND BDB_LIBRARIES)
     set(BDB_FOUND TRUE)
  endif (BDB_INCLUDE_DIR AND BDB_LIBRARIES)

  if (BDB_FOUND)
     if (NOT BDB_FIND_QUIETLY)
        message(STATUS "Found Berkeley DB: ${BDB_LIBRARIES}")
     endif (NOT BDB_FIND_QUIETLY)
  else (BDB_FOUND)
     if (BDB_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find Berkeley DB")
     endif (BDB_FIND_REQUIRED)
  endif (BDB_FOUND)

  MARK_AS_ADVANCED(BDB_INCLUDE_DIR BDB_LIBRARIES)

endif (BDB_INCLUDE_DIR AND BDB_LIBRARIES)
