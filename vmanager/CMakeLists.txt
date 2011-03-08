add_executable (vmanager main.cpp vmanagement.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (vmanager ${CONFIG_LIBRARIES} ${Boost_LIBRARIES})
