add_executable (provider provider.cpp page_manager.cpp pmgr_listener.cpp)
add_executable (sdht sdht.cpp page_manager.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (provider ${LIB_DEPENDENCIES})
target_link_libraries (sdht ${LIB_DEPENDENCIES})
