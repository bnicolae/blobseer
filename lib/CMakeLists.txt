add_library (blobseer-c SHARED blobseer.cpp)
add_library (blobseer SHARED ${BLOBSEER_SOURCE_DIR}/client/range_query.cpp ${BLOBSEER_SOURCE_DIR}/client/object_handler.cpp)

# Link the executable to the necessary libraries.
target_link_libraries (blobseer ${CONFIG_LIBRARIES} ${BOOST_LIBRARIES})
target_link_libraries (blobseer-c blobseer)
