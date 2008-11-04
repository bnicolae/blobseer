#include <iostream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;

int main(int argc, char **argv) {
    unsigned int page_size, replica_count;
    if (argc != 4 || sscanf(argv[2], "%u", &page_size) != 1 || sscanf(argv[3], "%u", &replica_count) != 1) {
	cout << "Usage: create_blob <config_file> <page_size> <replica_count>. To be used in empty deployment to generate ID 1." << endl;
	return 1;
    }

    object_handler *my_mem;
    my_mem = new object_handler(string(argv[1]));

    if (!my_mem->create(page_size, replica_count))
	cout << "Error: could not create the blob!" << endl;	
    else
	cout << "Blob created successfully." << endl;

    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
