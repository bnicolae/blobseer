# The name of our project is "BlobSeer". CMakeLists files in this project can
# refer to the root source directory of the project as ${BLOBSEER_SOURCE_DIR} and
# to the root binary directory of the project as ${BLOBSEER_BINARY_DIR}.
cmake_minimum_required (VERSION 2.6)
project (BLOBSEER)

# some global preprocessor definitions
add_definitions(-D__INFO -DSOCK_TYPE=tcp)

# set the include variables
set(EXTERNAL_INCLUDE ~/deploy/include)

# some global preprocessor definitions
add_definitions(-D__INFO -DSOCK_TYPE=tcp)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# find the OpenSSL library
find_package(OpenSSL)

# the elegant way to set up boost
#find_package(Boost COMPONENTS system)
#include_directories(${Boost_INCLUDE_DIRS})
#set(LIBS ${LIBS} ${Boost_LIBRARIES})

# temporary boost batch
set(GCCV gcc42)
find_library(BOOST_SYSTEM boost_system-${GCCV}-mt ~/deploy/lib)
find_library(BOOST_THREAD boost_thread-${GCCV}-mt ~/deploy/lib)
find_library(BOOST_SERIALIZATION boost_serialization-${GCCV}-mt ~/deploy/lib)
set(Boost_LIBRARIES ${BOOST_SYSTEM} ${BOOST_THREAD} ${BOOST_SERIALIZATION})

# set the libconfig++ library
find_library(config++_LIBRARIES config++ ~/deploy/lib)

# set the global link dependencies
set(LIB_DEPENDENCIES ${config++_LIBRARIES} ${Boost_LIBRARIES})

# Recurse through the directories
add_subdirectory (provider)
add_subdirectory (vmanager)
add_subdirectory (pmanager)
add_subdirectory (sdht)
add_subdirectory (lib)
add_subdirectory (test)