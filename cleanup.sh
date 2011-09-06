#!/bin/bash

make clean

find . -name CMakeFiles -exec rm -rf {} \;
find . -name MakeFiles -exec rm -rf {} \;
find . -name cmake_install -exec rm -rf {} \;
find . -name CMakeCache.txt -exec rm -rf {} \;
find . -name Makefile -exec rm -rf {} \;
find . -name cmake_install.cmake -exec rm -rf {} \;
find . -name *~ -exec rm -rf {} \;