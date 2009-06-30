#include <iostream>
#include <sstream>

#include "client/object_handler.hpp"

#include "common/debug.hpp"

using namespace std;
using namespace boost;

const uint64_t MAX_SIZE = 1 << 26; // 64 MB

int main(int argc, char **argv) {
    if (argc != 4) {
	cout << "Usage: <config_file> <id> <no_clients>" << endl;
	return 1;
    }

    object_handler *my_mem = new object_handler(string(argv[1]));
    char *big_zone = new char[MAX_SIZE];

    // read the command line arguments
    boost::uint64_t blob_id = 0, no_clients = 0, no_iterations = 0;
    std::istringstream sin(argv[2]);
    sin >> blob_id;
    sin.str(argv[3]);
    sin >> no_clients;
    
    // initialize the seed for the random number generator
    srand(time(NULL));
    while (true) {
	no_iterations++;
	if (!my_mem->get_latest(blob_id)) {
	    ERROR("could get latest blob version, aborting. Iterations completed: " << no_iterations);
	    return 1;
	}
	
	// some global parameters
	boost::uint64_t blob_size = my_mem->get_size();
	boost::uint64_t page_size = my_mem->get_page_size();
	boost::uint64_t blob_pages = blob_size / page_size;
	boost::uint64_t offset = 0, size = 0, pages = 0;

	// now flip a coin to choose what operation to complete
	int flip = rand() % 2;

	switch (flip) {
	case 0: // perform an append
	    INFO("now starting iteration " << no_iterations << ", performing an APPEND");

	    // first initialize the zone with some trash
	    for (boost::uint64_t i = 0; i < MAX_SIZE; i++)
		big_zone[i] = rand() % 256;
	    
	    // now try to perform the append
	    if (!my_mem->append(MAX_SIZE, big_zone))
		ERROR("could not append " << MAX_SIZE << " bytes");

	    INFO("iteration " << no_iterations << "(READ) ended");
	    break;

	case 1: // perform a read
	    INFO("now starting iteration " << no_iterations << ", performing a READ");	    
	    // if the blob does not contain a page yet, continue
	    if (blob_size < page_size)
		break;

	    // how many pages are we going to read? ... at least one, but the usual
	    // MapReduce approaches tries to split the blob evenly among clients, so
	    // we calculate the number of pages that make up our share
	    pages = blob_pages / no_clients;
	    if (pages == 0)
		pages = 1;

	    // read pages from a random position; make sure the position is valid
	    offset = (rand() % (blob_pages - pages + 1)) * page_size;
	    size = pages * page_size;

	    while (size > 0) {		
		boost::uint64_t read_size = size;
		if (read_size > MAX_SIZE)
		    read_size = MAX_SIZE;
		if (!my_mem->read(offset, read_size, big_zone))
		    ERROR("could not read (" << offset << ", " << read_size << ")");
		size -= read_size;
		break;
	    }
	    INFO("iteration " << no_iterations << "(READ) ended");
	    break;
	    
	default:
	    break;

	}

	// now do some virtual computation...assume it does not take longer than 15 secs on average
	sleep(rand() % 30);
    }

    // cleanup
    free(big_zone);
    delete my_mem;
    
    INFO("All done!");
    return 0;
}
