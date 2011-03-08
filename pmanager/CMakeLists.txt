add_executable (pmanager adv_manager.cpp publisher.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (pmanager ${CONFIG_LIBRARIES} ${Boost_LIBRARIES})
