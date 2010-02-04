#include <iostream>

#include "client/object_handler.hpp"

#include "common/debug.hpp"

#include <ctime>
#include <cstdlib>

using namespace std;
using namespace boost;

boost::uint64_t PAGE_SIZE;
boost::uint64_t TOTAL_PAGES;
boost::uint64_t REPLICA_COUNT;

bool test_mem(char *buff, boost::uint64_t size) {
    for (boost::uint64_t i = 0; i < size; i++)
	if (buff[i] != 'a')
	    return false;
    return true;
}

int main(int argc, char **argv) {
    unsigned int obj_id = 1;

    if (argc != 5
	|| sscanf(argv[2], "%ld", &PAGE_SIZE) != 1 
	|| sscanf(argv[3], "%ld", &TOTAL_PAGES) != 1
	|| sscanf(argv[4], "%ld", &REPLICA_COUNT) != 1) {
	cout << "Usage: appender <config_file> <page_size> <total_pages> <replica_count>" << endl;
	return 1;
    }
    boost::uint64_t TOTAL_SIZE = PAGE_SIZE * TOTAL_PAGES;

    char *big_zone = (char *)malloc(TOTAL_SIZE);
    srand(time(NULL));
    for (unsigned int i = 0; i < TOTAL_SIZE; i++)
	big_zone[i] = rand() % 256;

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
