# Find Boost libraries: requires Boost 1.40.0 or later
#  BOOST_FOUND - system has the BOOST library
#  BOOST_INCLUDE_DIR - the BOOST include directory
#  BOOST_LIBRARIES - The libraries needed to use BOOST

if (BOOST_INCLUDE_DIR AND BOOST_LIBRARIES)

  # in cache already
  SET(BOOST_FOUND TRUE)

else (BOOST_INCLUDE_DIR AND BOOST_LIBRARIES)
  FIND_PATH(BOOST_INCLUDE_DIR boost/version.hpp
     ${BOOST_ROOT}/include
     /usr/include/
     /usr/local/include/
     /sw/include
     /sw/local/include/
     $ENV{ProgramFiles}/BOOST/include/
     $ENV{SystemDrive}/BOOST/include/
  )

  if(WIN32 AND MSVC)
      # /MD and /MDd are the standard values - if somone wants to use
      # others, the libnames have to change here too
      # see http://www.BOOST.org/support/faq.html#PROG2 for their meaning

      FIND_LIBRARY(SSL_EAY_DEBUG NAMES ssleay32MDd
        PATHS
        $ENV{ProgramFiles}/BOOST/lib/VC/
        $ENV{SystemDrive}/BOOST/lib/VC/
      )
      FIND_LIBRARY(SSL_EAY_RELEASE NAMES ssleay32MD
        PATHS
        $ENV{ProgramFiles}/BOOST/lib/VC/
        $ENV{SystemDrive}/BOOST/lib/VC/
      )
      FIND_LIBRARY(LIB_EAY_DEBUG NAMES libeay32MDd
        PATHS
        $ENV{ProgramFiles}/BOOST/lib/VC/
        $ENV{SystemDrive}/BOOST/lib/VC/
      )
      FIND_LIBRARY(LIB_EAY_RELEASE NAMES libeay32MD
        PATHS
        $ENV{ProgramFiles}/BOOST/lib/VC/
        $ENV{SystemDrive}/BOOST/lib/VC/
      )

      IF(MSVC_IDE)
        IF(SSL_EAY_DEBUG AND SSL_EAY_RELEASE)
            SET(BOOST_LIBRARIES optimized ${SSL_EAY_RELEASE} ${LIB_EAY_RELEASE} debug ${SSL_EAY_DEBUG} ${LIB_EAY_DEBUG})
        ELSE(SSL_EAY_DEBUG AND SSL_EAY_RELEASE)
          MESSAGE(FATAL_ERROR "Could not find the debug and release version of BOOST")
        ENDIF(SSL_EAY_DEBUG AND SSL_EAY_RELEASE)
      ELSE(MSVC_IDE)
        STRING(TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_TOLOWER)
        IF(CMAKE_BUILD_TYPE_TOLOWER MATCHES debug)
          SET(BOOST_LIBRARIES ${SSL_EAY_DEBUG} ${LIB_EAY_DEBUG})
        ELSE(CMAKE_BUILD_TYPE_TOLOWER MATCHES debug)
          SET(BOOST_LIBRARIES ${SSL_EAY_RELEASE} ${LIB_EAY_RELEASE})
        ENDIF(CMAKE_BUILD_TYPE_TOLOWER MATCHES debug)
      ENDIF(MSVC_IDE)
      MARK_AS_ADVANCED(SSL_EAY_DEBUG SSL_EAY_RELEASE LIB_EAY_DEBUG LIB_EAY_RELEASE)
  else(WIN32 AND MSVC)
  
    FIND_LIBRARY(BOOST_SYSTEM   NAMES boost_system         PATHS ${BOOST_ROOT}/lib /usr/lib)
    FIND_LIBRARY(BOOST_THREAD   NAMES boost_thread         PATHS ${BOOST_ROOT}/lib /usr/lib)
    FIND_LIBRARY(BOOST_SER      NAMES boost_serialization  PATHS ${BOOST_ROOT}/lib /usr/lib)
    FIND_LIBRARY(BOOST_FS       NAMES boost_filesystem     PATHS ${BOOST_ROOT}/lib /usr/lib)
    FIND_LIBRARY(BOOST_DATETIME NAMES boost_date_time      PATHS ${BOOST_ROOT}/lib /usr/lib)
    
    if (BOOST_SYSTEM AND BOOST_THREAD AND BOOST_SER AND BOOST_FS AND BOOST_DATETIME)
	SET(BOOST_LIBRARIES ${BOOST_SYSTEM} ${BOOST_THREAD} ${BOOST_SER} ${BOOST_FS} ${BOOST_DATETIME})
    endif (BOOST_SYSTEM AND BOOST_THREAD AND BOOST_SER AND BOOST_FS AND BOOST_DATETIME)

  endif(WIN32 AND MSVC)

  if (BOOST_INCLUDE_DIR AND BOOST_LIBRARIES)
     set(BOOST_FOUND TRUE)
  endif (BOOST_INCLUDE_DIR AND BOOST_LIBRARIES)

  if (BOOST_FOUND)
     if (NOT BOOST_FIND_QUIETLY)
        message(STATUS "Found Boost: ${BOOST_LIBRARIES}")
     endif (NOT BOOST_FIND_QUIETLY)
  else (BOOST_FOUND)
     if (BOOST_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find Boost Libraries: system, thread, serialization, date_time, filesystem")
        message(FATAL_ERROR "Need Boost 1.40.0 or later")
     endif (BOOST_FIND_REQUIRED)
  endif (BOOST_FOUND)

  MARK_AS_ADVANCED(BOOST_INCLUDE_DIR BOOST_LIBRARIES)

endif (BOOST_INCLUDE_DIR AND BOOST_LIBRARIES)