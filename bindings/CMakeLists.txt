# Recurse through the directories

# FUSE bindings
set(FUSE_ROOT ${EXTERNAL_ROOT})
find_package(FUSE)

if (FUSE_FOUND)
  message(STATUS "Building FUSE support. FUSE headers: ${FUSE_INCLUDE_DIR}")
  add_subdirectory(blob-fuse)
else (FUSE_FOUND)
  message(STATUS "FUSE headers not found. FUSE support will not be built.")
endif (FUSE_FOUND)

# JAVA bindings
set(JNI_ROOT ${EXTERNAL_ROOT})
find_package(JNI)

if (JNI_INCLUDE_DIRS)
  message(STATUS "Building Java support. JNI headers: ${JAVA_INCLUDE_PATH}")
  add_subdirectory(java)
else (JNI_INCLUDE_DIRS)
  message(STATUS "JNI headers not found. Java support will not be built.")
endif (JNI_INCLUDE_DIRS)

# Ruby bindings
set(RUBY_ROOT ${EXTERNAL_ROOT})
find_package(Ruby)

if (RUBY_INCLUDE_PATH)
  message(STATUS "Building Ruby support. Ruby headers: ${RUBY_INCLUDE_PATH}")
  add_subdirectory (ruby)
else (RUBY_INCLUDE_PATH)
  message(STATUS "Ruby headers not found. Ruby support will not be built.")
endif (RUBY_INCLUDE_PATH)

# Python bindings
set(PYTHON_ROOT ${EXTERNAL_ROOT})
find_package(PythonLibs)

if (PYTHONLIBS_FOUND)
  find_package(Boost 1.40 QUIET COMPONENTS python)
  if (Boost_PYTHON_LIBRARY)
    message(STATUS "Building Python support. Python lib: ${Boost_PYTHON_LIBRARY}")
    add_subdirectory (python)
  else (Boost_PYTHON_LIBRARY)
    message(STATUS "Python headers not found. Python support will not be built.")
  endif (Boost_PYTHON_LIBRARY)
else (PYTHONLIBS_FOUND)
  message(STATUS "Python headers not found. Python support will not be built.")
endif (PYTHONLIBS_FOUND)
