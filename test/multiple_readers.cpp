#include <iostream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;
using namespace boost;

int main(int argc, char **argv) {
    unsigned int off_exp, size_exp, id, passes;
    if (argc != 7 
	|| sscanf(argv[3], "%u", &off_exp) != 1 || sscanf(argv[4], "%u", &size_exp) != 1
	|| sscanf(argv[5], "%u", &id) != 1 || sscanf(argv[6], "%u", &passes) != 1) {
	cout << "Usage: multiple_readers <config_file> <sync_file> <offset_exp> <size_exp> <id> <passes>." << endl;
	return 1;
    }

    // size = 2 ^ size_exp; offset = position in blob * size
    uint64_t size = (uint64_t)1 << size_exp;
    uint64_t off = off_exp * size;

    // give nice names to args so as to not get confused :)
    string cfg_file(argv[1]);
    char *sync_file = argv[2];

    // alloc chunk size and fill it with irrelevant data
    srand(time(NULL));
    char *big_zone = (char *)malloc(size); 
    for (unsigned int i = 0; i < size; i++) 
	big_zone[i] = rand() % 256;

    object_handler *my_mem;
    my_mem = new object_handler(cfg_file);
    
    // busy wait till the sync file is visible
    while(access(sync_file, F_OK) != 0);

    // now try to perform the passes
    if (my_mem->get_latest(id)) 
	for (int i = 0; i < passes; i++) {
	    cout << "WRITE: pass " << i << " out of " << passes << ":" << endl;
	    if (!my_mem->write(off, size, big_zone))
		cout << "WRITE: pass " << i << " FAILED!" << endl;
	    else
		cout << "WRITE: pass " << i << " OK!" << endl;
	}
    else
	cout << "Could not initiate WRITE: get_latest() failed" << endl;
    
    // some cleanup
    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
