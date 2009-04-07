#include <iostream>
#include <fstream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;

int main(int argc, char **argv) {

  unsigned int page_size, replica_count;

  // ------------------------------------------------------------------
  // Checking input parameters
  if (argc != 5 ||
      sscanf(argv[2], "%u", &page_size) != 1 ||
      sscanf(argv[3], "%u", &replica_count) != 1) {
    cout << "Usage: upload_file <config_file> <page_size> <replica_count> <data file>" << endl;
    return 1;
  }
  string conf_file = string(argv[1]);
  char* data_file = argv[4];

  // ------------------------------------------------------------------
  // Creating blob
  object_handler *my_mem;
  my_mem = new object_handler(conf_file);

  if (!my_mem->create(page_size, replica_count)) {
    cout << "Error: could not create the blob!" << endl;
    return 2;
  }

  // ------------------------------------------------------------------
  // Uploading data
  cout << "Uploading file " << data_file << "..." << endl;
  char* buffer = (char*)malloc(page_size*sizeof(char));
  ifstream file (data_file, ios::in|ios::binary);

  // get length of file:
  file.seekg (0, ios::end);
  int length = file.tellg();
  file.seekg (0, ios::beg);

  int pos = 0;
  int steps = 0;
  while ((pos < length) && !file.eof()) {
    file.read(buffer, page_size);
    //bool res = my_mem->write(pos, page_size, buffer);
    bool res = my_mem->append(page_size, buffer);
    if (!res)
      cout << "ERROR! " << page_size << "bytes could not be uploaded" << endl;
    pos += page_size;
    steps++;
  }
  cout << "FINISHED: "<< pos << " bytes uploaded in blocks of " << page_size << " bytes (" << steps << " iterations)"<< endl;

  // ------------------------------------------------------------------
  // Finishing and closing stuff
  cout << "FINISHED: Blob created with id = " << my_mem->get_id() << "and size = "<< my_mem->get_size() << endl;

  delete my_mem;

  return 0;
}
