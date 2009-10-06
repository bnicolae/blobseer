#include <iostream>

#include "client/object_handler.hpp"

#include "common/debug.hpp"

using namespace std;
using namespace boost;

const boost::uint64_t STOP_SIZE = 65536;
const boost::uint64_t TOTAL_SIZE = 65536;
const boost::uint64_t PAGE_SIZE = 1024;
const boost::uint64_t READ_SIZE = 128;
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
    
    char *big_zone = (char *)malloc(STOP_SIZE);
    memset(big_zone, 'a', STOP_SIZE);

    object_handler *my_mem = new object_handler(string(argv[1]));

    if ((!my_mem->get_latest(obj_id) || my_mem->get_page_size() == 0)
	&& !my_mem->create(PAGE_SIZE, REPLICA_COUNT)) {
	ERROR("Could not initialize blob id " << obj_id);
	return 1;
    } else
	INFO("get_latest() successful, now starting...");

    INFO("PAGE SIZE = " << PAGE_SIZE << ", TOTAL WRITE SIZE = " << TOTAL_SIZE);
    
    if (!my_mem->append(3 * PAGE_SIZE, big_zone)) {
	ERROR("Could not append!");
	return 2;
    }

    memset(big_zone, 'b', STOP_SIZE);
    INFO("Testing unaligned read: aligned left, unaligned right, single page...");
    if (!my_mem->get_latest() || !my_mem->read(0, PAGE_SIZE - 1, big_zone) || !test_mem(big_zone, PAGE_SIZE - 1)) {
	ERROR("FAILED");
	return 3;
    }

    memset(big_zone, 'b', STOP_SIZE);
    INFO("Testing unaligned read: unaligned left, unaligned right, single page...");
    if (!my_mem->get_latest() || !my_mem->read(1, PAGE_SIZE - 2, big_zone) || !test_mem(big_zone, PAGE_SIZE - 2)) {
	ERROR("FAILED");
	return 4;
    }

    memset(big_zone, 'b', STOP_SIZE);
    INFO("Testing unaligned read: unaligned left, aligned right, single page ...");
    if (!my_mem->get_latest() || !my_mem->read(1, PAGE_SIZE - 1, big_zone) || !test_mem(big_zone, PAGE_SIZE - 1)) {
	ERROR("FAILED");
	return 5;
    }

    memset(big_zone, 'b', STOP_SIZE);
    INFO("Testing unaligned read: aligned left, unaligned right, multiple pages...");
    if (!my_mem->get_latest() || !my_mem->read(0, 3 * PAGE_SIZE - 1, big_zone) || !test_mem(big_zone, 3 * PAGE_SIZE - 1)) {
	ERROR("FAILED");
	return 6;
    }

    memset(big_zone, 'b', STOP_SIZE);
    INFO("Testing unaligned read: unaligned left, unaligned right, multiple pages...");
    if (!my_mem->get_latest() || !my_mem->read(1, 3 * PAGE_SIZE - 2, big_zone) || !test_mem(big_zone, 3 * PAGE_SIZE - 2)) {
	ERROR("FAILED");
	return 7;
    }

    memset(big_zone, 'b', STOP_SIZE);
    INFO("Testing unaligned read: unaligned left, aligned right, multiple pages ...");
    if (!my_mem->get_latest() || !my_mem->read(1, 3 * PAGE_SIZE - 1, big_zone) || !test_mem(big_zone, 3 * PAGE_SIZE - 1)) {
	ERROR("FAILED");
	return 8;
    }

    free(big_zone);
    delete my_mem;

    INFO("All tests passed SUCCESSFULLY");
    return 0;
}
