#include <iostream>

#include "client/object_handler.hpp"

#include "common/debug.hpp"

using namespace std;
using namespace boost;

const boost::uint64_t PAGE_SIZE = 8;
const boost::uint64_t TOTAL_SIZE = 4 * PAGE_SIZE;
const boost::uint64_t REPLICA_COUNT = 1;

int main(int argc, char **argv) {
    if (argc != 2) {
	cout << "Usage: clone_test <config_file>" << endl;
	return 1;
    }
    
    char *big_zone = (char *)malloc(TOTAL_SIZE);
    memset(big_zone, 'a', TOTAL_SIZE);

    object_handler *my_mem = new object_handler(string(argv[1]));
    
    if (!my_mem->create(PAGE_SIZE, REPLICA_COUNT) || 
	!my_mem->append(3 * PAGE_SIZE, big_zone)) {
	ERROR("could not create initial blob");
    }

    unsigned int original = my_mem->get_id(), first = 0, second = 0;
    if (!my_mem->clone(original, 1)) {
	ERROR("could not create first clone");
	return 1;
    } else {
	first = my_mem->get_id();
	INFO("first clone created successfully, id: " << first);
    }

    if (!my_mem->clone(original, 1)) {
	ERROR("could not create second clone");
	return 1;
    } else {
	second = my_mem->get_id();
	INFO("second clone created successfully, id: " << second);
    }

    memset(big_zone, 'b', PAGE_SIZE);
    if (!my_mem->append(PAGE_SIZE, big_zone))
	ERROR("could not append to second clone");

    if (!my_mem->get_latest(first))
	ERROR("could not switch back to first clone");
    memset(big_zone, 'c', PAGE_SIZE);
    if (!my_mem->write(PAGE_SIZE, PAGE_SIZE, big_zone))
	ERROR("could not write second page of first clone");
    
    if (!my_mem->get_latest(second))
	ERROR("could not switch back to second clone");
    memset(big_zone, 'd', PAGE_SIZE);
    if (!my_mem->write(2 * PAGE_SIZE, PAGE_SIZE, big_zone))
	ERROR("could not write third page of second clone");

    if (!my_mem->get_latest(first))
	ERROR("could not switch back to first clone");
    memset(big_zone, 'e', PAGE_SIZE);
    if (!my_mem->write(2 * PAGE_SIZE, PAGE_SIZE, big_zone))
	ERROR("could not write third page of first clone");

    memset(big_zone, 'x', TOTAL_SIZE);
    cout << "Results are as follows: " << endl;

    if (!my_mem->get_latest(original))
	ERROR("could not switch back to original");
    if (!my_mem->read(0, 3 * PAGE_SIZE, big_zone))
	ERROR("could not read original");
    cout << "ORIGINAL: " << big_zone << endl;
    
    if (!my_mem->get_latest(first))
	ERROR("could not switch back to first clone");
    if (!my_mem->read(0, 3 * PAGE_SIZE, big_zone))
	ERROR("could not read first clone");
    cout << "FIRST CLONE: " << big_zone << endl;

    if (!my_mem->get_latest(second))
	ERROR("could not switch back to second clone");
    if (!my_mem->read(0, 4 * PAGE_SIZE, big_zone))
	ERROR("could not read second clone");
    cout << "SECOND CLONE: " << big_zone << endl;

    free(big_zone);
    delete my_mem;

    INFO("Clone test completed!");
    return 0;
}
