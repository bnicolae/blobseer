cmake_minimum_required(VERSION 2.6)

# Adding libs and includes to the environment
include_directories(${PYTHON_INCLUDE_PATH})
link_libraries(boost_python)
link_directories(${PYTHON_LIBRARIES})

# Target
add_library(pyblobseer SHARED pyblobseer.cpp)
add_dependencies(pyblobseer blobseer)
target_link_libraries(pyblobseer blobseer)
set_target_properties(pyblobseer PROPERTIES PREFIX "")
