add_executable (pmanager adv_manager.cpp publisher.cpp)
target_link_libraries (pmanager ${CONFIG_LIBRARIES} ${Boost_LIBRARIES})
install (TARGETS pmanager RUNTIME DESTINATION bin)
