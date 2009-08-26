#include <iostream>
#include "client/object_handler.hpp"

using namespace std;
using namespace boost;

const uint64_t PAGE_SIZE = 1024;
const uint64_t BUFFER_SIZE = 128;

int main(int argc, char **argv) {
    unsigned int obj_id = 0;

    if (argc != 3 || sscanf(argv[1], "%d", &obj_id) != 1) {
	cout << "Usage: test <id> <config_file>. Create the blob with create_blob first." << endl;
	return 1;
    }
    
    char *big_zone = (char *)malloc(BUFFER_SIZE); 

    object_handler *my_mem = new object_handler(string(argv[2]));

    if (!my_mem->get_latest(obj_id)) {
	cout << "Could not alloc latest version, write test aborting" << endl;
	return 1;
    } else
	cout << "get_latest() successful, now starting..." << endl;

   
    my_mem->read(0, BUFFER_SIZE, big_zone);
		
    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
