#include <iostream>
#include "client/object_handler.hpp"

using namespace std;
using namespace boost;

const uint64_t PAGE_SIZE = 1 << 16; // 64 KB
const uint64_t START_SIZE = 1 << 20; // 1 MB
const uint64_t STOP_SIZE = 1 << 29; // 512 MB

int main(int argc, char **argv) {
    unsigned int obj_id = 0;
    char operation;

    if (argc != 4 || sscanf(argv[1], "%c", &operation) != 1 || sscanf(argv[2], "%d", &obj_id) != 1) {
	cout << "Usage: test <op> <id> <config_file>. Create the blob with create_blob first. op=R/W/A (read/write/append)" << endl;
	return 1;
    }
    
    char *big_zone = (char *)malloc(STOP_SIZE); 

    object_handler *my_mem = new object_handler(string(argv[3]));

    if (!my_mem->get_latest(obj_id)) {
	cout << "Could not alloc latest version, write test aborting" << endl;
	return 1;
    } else
	cout << "get_latest() successful, now starting..." << endl;
    if (operation == 'W') {
	char c = '0';
	for (uint64_t offset = 0, size = START_SIZE; size <= STOP_SIZE; offset += size, size <<= 1) {
	    memset(big_zone, c++, size);
	    if (!my_mem->write(offset, size, big_zone))
		cout << "Could not write (" << offset << ", " << size << ")" << endl;
	}	
    } else if (operation == 'R') {
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
    } else {
	char c = '0';
	for (uint64_t offset = 0; offset < STOP_SIZE; offset += PAGE_SIZE) {
	    memset(big_zone, c++, PAGE_SIZE);
	    if (!my_mem->append(PAGE_SIZE, big_zone))
		cout << "Could not append at " << offset << endl;
	}
    }

    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
