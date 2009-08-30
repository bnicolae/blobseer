#include <iostream>
#include "client/object_handler.hpp"

using namespace std;
using namespace boost;

const boost::uint64_t STOP_SIZE = 65536;
const boost::uint64_t TOTAL_SIZE = 1024;
const boost::uint64_t PAGE_SIZE = 1024;
const boost::uint64_t READ_SIZE = 128;
const boost::uint64_t REPLICA_COUNT = 1;

int main(int argc, char **argv) {
    unsigned int obj_id = 1;

    if (argc != 2) {
	cout << "Usage: basic_test <config_file>" << endl;
	return 1;
    }
    
    char *big_zone = (char *)malloc(STOP_SIZE);
    memset(big_zone, 'a', STOP_SIZE);

    object_handler *my_mem = new object_handler(string(argv[1]));

    if ((!my_mem->get_latest(obj_id) || my_mem->get_page_size() == 0)
	&& !my_mem->create(PAGE_SIZE, REPLICA_COUNT)) {
	cout << "Could not initialize blob id " << obj_id << endl;
	return 1;
    } else
	cout << "get_latest() successful, now starting..." << endl;

    cout << "PAGE SIZE = " << PAGE_SIZE << ", TOTAL WRITE SIZE = " << TOTAL_SIZE << endl;
    
    if (!my_mem->append(TOTAL_SIZE, big_zone))
	cout << "Could not append!";

    memset(big_zone, 'b', STOP_SIZE);
    if (!my_mem->get_latest() || !my_mem->read(0, READ_SIZE, big_zone))
	cout << "Could not read!";

    cout << "Buffer contents after read:" << endl;
    for (unsigned int i = 0; i < READ_SIZE; i++)
	cout << big_zone[i];
    cout << endl;

    free(big_zone);
    delete my_mem;

    cout << "End of test" << endl;
    return 0;
}
