#include <iostream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;
using namespace boost;

int main(int argc, char **argv) {
    unsigned int off, size, id, passes;
    if (argc != 5 || sscanf(argv[2], "%u", &off) != 1 || sscanf(argv[3], "%u", &size) != 1 
	|| sscanf(argv[5], "%u", &id) != 1 || sscanf(argv[6], "%u", &passes)) {
	cout << "Usage: multiple_readers <config_file> <offset> <size> <sync_file> <id> <passes>." << endl;
	return 1;
    }

    // alloc chunk size
    char *big_zone = (char *)malloc(size); 

    object_handler *my_mem;
    my_mem = new object_handler(string(argv[1]));
    
    while(access(argv[4], F_OK) != 0);

    if (my_mem->get_latest(id)) 
	for (int i = 0; i < passes; i++) {	    
	    cout << "Pass " << i << " out of " << passes << ":" << endl;
	    if (!my_mem->read(off, size, big_zone))
		cout << "Pass " << i << " FAILED!" << endl;
	    else
		cout << "Pass " << i << " OK!" << endl;
	} 
    else
	cout << "Could not initiate READ: get_latest() failed" << endl;
    
    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
