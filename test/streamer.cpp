#include <iostream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;

int main(int argc, char **argv) {
    unsigned int page_size_exp, replica, max_size_exp, step_exp;
    uint64_t page_size, max_size, step;
    
    if (argc != 6 || sscanf(argv[2], "%u", &page_size_exp) != 1 || sscanf(argv[3], "%u", &replica) != 1 
	|| sscanf(argv[4], "%u", &max_size_exp) != 1 || sscanf(argv[5], "%u", &step_exp) != 1
	) {
	cout << "Usage: streamer <config_file> <page_size> <replica> <max_size> <step>" << endl;
	return 1;
    }
    page_size = 1 << page_size_exp;
    max_size = 1 << max_size_exp;
    step = 1 << step_exp;

    // alloc chunk size
    char *big_zone = (char *)malloc(step); 
    for (uint64_t i = 0; i < step; i++) 
	big_zone[i] = i % 256;

    object_handler *my_mem;
    my_mem = new object_handler(string(argv[1]));

    if (!my_mem->create(page_size, replica))
	cout << "ERROR: could not create blob" << endl;
    else
	cout << "SUCCESS: blob created, id = " << my_mem->get_id() << endl;

    boost::posix_time::ptime timer(boost::posix_time::microsec_clock::local_time());
    for (uint64_t offset = 0; offset < max_size; offset += step)
	my_mem->write(offset, step, big_zone);
    boost::posix_time::time_duration t = boost::posix_time::microsec_clock::local_time() - timer;
    cout << "SUCCESS: stream completed, time: " << t << endl;
    
    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
