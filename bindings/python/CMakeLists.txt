# Adding libs and includes to the environment
include_directories(${PYTHON_INCLUDE_PATH})

# Target
add_library(pyblobseer SHARED pyblobseer.cpp)
target_link_libraries(pyblobseer blobseer ${Boost_PYTHON_LIBRARY})
