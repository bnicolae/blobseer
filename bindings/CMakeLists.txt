# Recurse through the directories

# Now handle the JAVA bindings
find_package(JNI)
if (JNI_INCLUDE_DIRS)
  message(STATUS "Building Java support. JNI headers: ${JAVA_INCLUDE_PATH}")
  add_subdirectory (java)
else (JNI_INCLUDE_DIRS)
  message(STATUS "JNI headers not found. Java support will not be built.")
endif (JNI_INCLUDE_DIRS)

# Ruby bindings
find_package(Ruby)
if (RUBY_INCLUDE_PATH)
  message(STATUS "Building Ruby support. Ruby headers: ${RUBY_INCLUDE_PATH}")
  add_subdirectory (ruby)
else (RUBY_INCLUDE_PATH)
  message(STATUS "Ruby headers not found. Ruby support will not be built.")
endif (RUBY_INCLUDE_PATH)

# Python bindings
#find_package(PythonLibs)
if (PYTHON_INCLUDE_PATH)
  find_package(Boost 1.36 QUIET COMPONENTS python)
  if (Boost_PYTHON_LIBRARY)
    message(STATUS "Building Python support. Python lib: ${Boost_PYTHON_LIBRARY}")
    add_subdirectory (python)
  else (Boost_PYTHON_LIBRARY)
    message(STATUS "Python headers not found. Python support will not be built.")
  endif (Boost_PYTHON_LIBRARY)
else (PYTHON_INCLUDE_PATH)
  message(STATUS "Python headers not found. Python support will not be built.")
endif (PYTHON_INCLUDE_PATH)
