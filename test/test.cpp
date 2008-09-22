#include <iostream>
#include "client/object_handler.hpp"
#include "common/configuration.hpp"

using namespace std;
using namespace boost;

const uint64_t MAX_SIZE = (uint64_t)1 << 40; // 1 TB
const uint64_t START_SIZE = 1 << 16; // 64 KB
const uint64_t STOP_SIZE = 1 << 28; // 1 GB

int main(int argc, char **argv) {
    unsigned int obj_id = 0;
    char operation = 'R';

    if (argc != 4 || sscanf(argv[1], "%c", &operation) != 1 || sscanf(argv[2], "%d", &obj_id) != 1) {
	cout << "Usage: test <op> <id> <config_file>" << endl;
	return 1;
    }
    // alloc 1Gb
    char *big_zone = (char *)malloc(STOP_SIZE); 

    object_handler *my_mem = new object_handler(string(argv[3]));
    
    if (operation == 'W') {
	if (!my_mem->alloc(START_SIZE, MAX_SIZE)) {
	    cout << "Could not alloc latest version, write test aborting" << endl;
	    return 1;
	} 
	cout << "Alloc successful, now starting write..." << endl;	    
	char c = '0';
	for (uint64_t offset = 0, size = START_SIZE; size <= STOP_SIZE; offset += size, size <<= 1) {
	    memset(big_zone, c++, size);
	    if (!my_mem->write(offset, size, big_zone))
		cout << "Could not write (" << offset << ", " << size << ")" << endl;
	}	
    } else {
	if (!my_mem->get_latest(true, obj_id)) {
	    cout << "Could not bind to latest version, read test aborting" << endl;
	    return 2;
	}	
	char c = '0';
	for (uint64_t offset = 0, size = START_SIZE; size <= STOP_SIZE; offset += size, size <<= 1) {
	    if (!my_mem->read(offset, size, big_zone))
		cout << "Could not read (" << offset << ", " << size << ")" << endl;
	    cout << "Checking returned result...";
	    uint64_t i;
	    for (i = 0; i < size; i++)
		if (big_zone[i] != c) {
		    cout << "Offset " << i << " failed!" << endl;
		    break;
		}
	    if (i == size)
		cout << "OK!" << endl;
	    c++;
	}
    }

    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
