#include <iostream>

#include <sys/mman.h>

#include "client/object_handler.hpp"

#include "common/debug.hpp"

using namespace std;

const boost::uint64_t MAX_PAGE_SIZE = 1 << 20, REPLICA_COUNT = 1;

int main(int argc, char **argv) {
    if (argc != 3) {
	cout << "Usage: " << argv[0] << " <file_name> <config_file>" << endl;
	return 1;
    }

    struct stat file_stat;
    if (stat(argv[1], &file_stat)) {
	ERROR("Could not stat input file: " << argv[1]);
	return 2;
    }
    boost::uint64_t file_size = file_stat.st_size;
    int fd = open(argv[1], O_RDONLY);
    if (fd == -1) {
	ERROR("Could not open input file: " << argv[1]);
	return 2;
    }

    // determining page size
    boost::uint64_t page_size = 1;
    while (file_size % (page_size << 1) == 0 && page_size < MAX_PAGE_SIZE)
	page_size <<= 1;
    INFO("Used page size is: " << page_size);
    
    char *region = (char *)mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if ((void *)region == (void *) -1) {
	ERROR("Could not mmap file: " << argv[1] << ", aborting!");
	return 3;
    }
    
    object_handler my_blob(argv[2]);

    if(my_blob.create(page_size, REPLICA_COUNT)) {
	INFO("Now writing file to blob id " << my_blob.get_id());
	if (my_blob.append(file_size, region))
	    INFO("Operation successful!");
	else
	    ERROR("Operation failed!");
    } else
	ERROR("Couln't create blob successfully!");

    munmap(region, file_size);
    close(fd);

    INFO("Cleanup complete");
    return 0;
}
