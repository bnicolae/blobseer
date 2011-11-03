add_executable (vmanager main.cpp vmanagement.cpp)
target_link_libraries (vmanager ${CONFIG_LIBRARIES} ${Boost_LIBRARIES})
install (TARGETS vmanager RUNTIME DESTINATION bin)
