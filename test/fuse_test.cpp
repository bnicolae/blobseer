#include <iostream>

#include "client/object_handler.hpp"
#include "bindings/blob-fuse/blob_ioctl.hpp"
#include "common/debug.hpp"

using namespace std;

int main(int argc, char **argv) {
    if (argc != 2) {
	cout << "Usage: fuse_test <blob_fuse_input_file>" << endl;
	return 1;
    }

    cout << "Starting FUSE test!" << endl;
    
    cout << "Opening file...";
    int fd = open(argv[1], O_RDWR);
    if (fd == -1) {
	cout << "FAILED!" << endl;
	return 2;
    } else
	cout << "OK!" << endl;

    cout << "Writing file...";
    char buffer[] = "ababababab";
    if (lseek(fd, 10, SEEK_SET) != 10 || write(fd, buffer, 10) != 10) {
	cout << "FAILED!" << endl;
	return 2;
    } else
	cout << "OK!" << endl;

    boost::uint64_t args;
    cout << "CLONE_AND_COMMIT..." << ioctl(fd, CLONE_AND_COMMIT, &args) << endl;
    perror("Latest error: ");

    cout << "FUSE test completed!" << endl;
    return 0;
}
