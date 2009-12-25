#include <iostream>

#include "client/object_handler.hpp"

#include "common/debug.hpp"

using namespace std;
using namespace boost;

boost::uint64_t PAGE_SIZE;
boost::uint64_t TOTAL_PAGES;

bool test_mem(char *buff, boost::uint64_t size) {
    for (boost::uint64_t i = 0; i < size; i++)
	if (buff[i] != 'a')
	    return false;
    return true;
}

int main(int argc, char **argv) {
    unsigned int obj_id = 1;
    boost::uint64_t offset;

    if (argc != 5 || sscanf(argv[2], "%ld", &offset) != 1 || sscanf(argv[3], "%ld", &PAGE_SIZE) != 1 || sscanf(argv[4], "%ld", &TOTAL_PAGES) != 1) {
	cout << "Usage: reader <config_file> <offset> <page_size> <total_pages>" << endl;
	return 1;
    }    
    boost::uint64_t TOTAL_SIZE = PAGE_SIZE * TOTAL_PAGES;
    
    char *big_zone = (char *)malloc(TOTAL_SIZE);
    object_handler *my_mem = new object_handler(string(argv[1]));

    if (!my_mem->get_latest(obj_id)) {
	ERROR("Could not get latest version for blob id " << obj_id);
	return 1;
    } else
	INFO("BLOB SIZE = " << my_mem->get_size() << ", PAGE SIZE = " << PAGE_SIZE << ", OFFSET = " << offset << ", TOTAL READ SIZE = " << TOTAL_SIZE);

    if (!my_mem->read(offset, TOTAL_SIZE, big_zone)) {
	ERROR("Could not read!");
	return 2;
    }
    INFO("all OK!");

    return 0;
}
