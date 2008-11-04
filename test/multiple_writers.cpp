#include <iostream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;

const uint64_t MAX_SIZE = (uint64_t)1 << 40; // 1 TB
const uint64_t PAGE_SIZE = 1 << 16; // 64 KB

int main(int argc, char **argv) {
    unsigned int off, size, passes, max_size;
    if (argc != 6 || sscanf(argv[3], "%u", &off) != 1 || sscanf(argv[4], "%u", &size) != 1 
	|| sscanf(argv[5], "%u", &passes) != 1 || sscanf(argv[5], "%u", &max_size) != 1) {
	cout << "Usage: multiple_writers <config_file> <sync_file> <offset> <max_size> <passes> <max_size>. Assumes ID is 1." << endl;
	return 1;
    }

    // alloc chunk size
    srand(time(NULL));
    char *big_zone = (char *)malloc(max_size); 
    for (unsigned int i = 0; i < max_size; i++) 
	big_zone[i] = i % 256;

    object_handler *my_mem;
    my_mem = new object_handler(string(argv[1]));
    
    while(access(argv[4], F_OK) != 0);

    if (!my_mem->get_latest(1))
	cout << "ERROR: could not locate the blob with ID 1" << endl;
    uint64_t page_size = my_mem->get_page_size();

    while (my_mem->get_size() < max_size) {
	
    }
    for (unsigned int i = 0; i < passes; i++) {
	uint64_t size = (rand() % (max_size / page_size)) * page_size;
	cout << "Pass " << i << " out of " << passes << "; size is " << size << ":" << endl;
	
	if (!my_mem->write(off, size, big_zone))
	    cout << "Pass " << i << " FAILED!" << endl;
	else
	    cout << "Pass " << i << " OK!" << endl;
    }
    
    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
