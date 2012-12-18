# The name of our project is "BlobSeer". CMakeLists files in this project can
# refer to the root source directory of the project as ${BLOBSEER_SOURCE_DIR} and
# to the root binary directory of the project as ${BLOBSEER_BINARY_DIR}.
cmake_minimum_required (VERSION 2.8)
project (BLOBSEER)

# set the root prefix of the library dependencies
set(EXTERNAL_ROOT ~/deploy)

# some global compiler flags
add_definitions(-O2 -Wall -D__ASSERT -D__INFO -D__BENCHMARK -DSOCK_TYPE=tcp)

# set the path to the cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# Now set up the libraries
# -----------------------------------------------------------------------------------

# the elegant way to set up boost
set(BOOST_ROOT ${EXTERNAL_ROOT})
find_package(Boost 1.40 COMPONENTS date_time filesystem system thread serialization)
include_directories(${Boost_INCLUDE_DIR})

find_package(Threads)
set(Boost_LIBRARIES ${Boost_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

# set up the libconfig++ library
set(CONFIG_ROOT ${EXTERNAL_ROOT})
find_package(Config)
include_directories(${CONFIG_INCLUDE_DIR})

# set up Berkley DB
set(BDB_ROOT ${EXTERNAL_ROOT})
find_package(BerkeleyDB)
include_directories(${BDB_INCLUDE_DIR})

# set up OpenSSL
find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

# -----------------------------------------------------------------------------------

# set the global include dirs
include_directories(${BLOBSEER_SOURCE_DIR})

# Server side
add_subdirectory (provider)
add_subdirectory (vmanager)
add_subdirectory (pmanager)

# Client side
add_subdirectory (lib)

# Bindings to various languages other than C/C++
add_subdirectory (bindings)

# BlobVM
add_subdirectory (blob-vm)

# Tests
add_subdirectory (test)
