# The name of our project is "BlobSeer". CMakeLists files in this project can
# refer to the root source directory of the project as ${BLOBSEER_SOURCE_DIR} and
# to the root binary directory of the project as ${BLOBSEER_BINARY_DIR}.
cmake_minimum_required (VERSION 2.6)
project (BLOBSEER)

# set the root prefix of the library dependencies
set(EXTERNAL_ROOT ~/deploy)

# some global compiler flags
add_definitions(-O2 -Wall -D__ASSERT -D__INFO -D__BENCHMARK -DWITH_MUTEX -DSOCK_TYPE=tcp)

# set the path to the cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# Now set up the libraries
# -----------------------------------------------------------------------------------

# find the OpenSSL library
set(OPENSSL_ROOT ${EXTERNAL_ROOT})
find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

# the elegant way to set up boost
set(BOOST_ROOT ${EXTERNAL_ROOT})
set(Boost_ADDITIONAL_VERSIONS "1.37" "1.38" "1.39")
find_package(Boost 1.36 REQUIRED COMPONENTS system thread serialization filesystem date_time)
include_directories(${Boost_INCLUDE_DIRS})

# set up the libconfig++ library
set(CONFIG_ROOT ${EXTERNAL_ROOT})
find_package(Config)
include_directories(${CONFIG_INCLUDE_DIR})

# set up Berkley DB
set(BDB_ROOT ${EXTERNAL_ROOT})
find_package(BerkeleyDB)
include_directories(${BDB_INCLUDE_DIR})

#set up JNI
find_package(JNI)

# -----------------------------------------------------------------------------------

# set the global link dependencies and include dirs
set(LIB_DEPENDENCIES ${CONFIG_LIBRARIES} ${Boost_LIBRARIES})
include_directories(${BLOBSEER_SOURCE_DIR})

# Recurse through the directories
add_subdirectory (provider)
add_subdirectory (vmanager)
add_subdirectory (pmanager)
add_subdirectory (lib)
add_subdirectory (test)
if (JNI_INCLUDE_DIRS)
  message(STATUS "Bulding Java support. JNI headers: ${JAVA_INCLUDE_PATH}")
  add_subdirectory (java)
else (JNI_INCLUDE_DIRS)
  message(STATUS "JNI headers not found. Java support will not be built.")
endif (JNI_INCLUDE_DIRS)