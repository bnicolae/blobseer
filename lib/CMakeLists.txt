add_library (jerasure SHARED jerasure.c galois.c)
add_library (blobseer-c SHARED blobseer.cpp)
add_library (blobseer-f SHARED blobseerf.cpp)
add_library (blobseer SHARED 
  ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp 
  ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp
  ${BLOBSEER_SOURCE_DIR}/client/ft_replication.cpp
  ${BLOBSEER_SOURCE_DIR}/client/ft_erasure.cpp
)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer jerasure ${CONFIG_LIBRARIES} ${Boost_LIBRARIES} ${OPENSSL_LIBRARIES})
target_link_libraries (blobseer-c blobseer)
target_link_libraries (blobseer-f blobseer)

# Install libraries
install (TARGETS blobseer blobseer-c blobseer-f 
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
install (FILES
  ${BLOBSEER_SOURCE_DIR}/lib/blobseer.h
  DESTINATION include
)
