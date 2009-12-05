#include <iostream>

#include "client/object_handler.hpp"

#include "common/debug.hpp"

using namespace std;
using namespace boost;

const boost::uint64_t PAGE_SIZE = 65536;
const boost::uint64_t TOTAL_SIZE = 100 * PAGE_SIZE;
const boost::uint64_t REPLICA_COUNT = 1;

bool test_mem(char *buff, boost::uint64_t size) {
    for (boost::uint64_t i = 0; i < size; i++)
	if (buff[i] != 'a')
	    return false;
    return true;
}

int main(int argc, char **argv) {
    unsigned int obj_id = 1;

    if (argc != 2) {
	cout << "Usage: basic_test <config_file>" << endl;
	return 1;
    }
    
    char *big_zone = (char *)malloc(TOTAL_SIZE);
    memset(big_zone, 'a', TOTAL_SIZE);

    object_handler *my_mem = new object_handler(string(argv[1]));

    if ((!my_mem->get_latest(obj_id) || my_mem->get_page_size() == 0)
	&& !my_mem->create(PAGE_SIZE, REPLICA_COUNT)) {
	ERROR("Could not initialize blob id " << obj_id);
	return 1;
    } else
	INFO("get_latest() successful, now starting...");

    INFO("PAGE SIZE = " << PAGE_SIZE << ", TOTAL WRITE SIZE = " << TOTAL_SIZE);

    if (!my_mem->append(TOTAL_SIZE, big_zone)) {
	ERROR("Could not append!");
	return 2;
    }
    INFO("all OK!");

    return 0;
}
