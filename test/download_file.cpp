#include <iostream>
#include <fstream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;

int main(int argc, char **argv) {

  unsigned int page_size, blob_id;

  // ------------------------------------------------------------------
  // Checking input parameters
  if (argc != 5 ||
      sscanf(argv[2], "%u", &page_size) != 1 ||
      sscanf(argv[3], "%u", &blob_id) != 1) {
    cout << "Usage: dowload_file <config_file> <page size> <blob id> <output file>" << endl;
    return 1;
  }
  string conf_file = string(argv[1]);
  char* output_file = argv[4];

  // ------------------------------------------------------------------
  // Obtaining blob
  object_handler *my_mem;
  my_mem = new object_handler(conf_file);

  if (!my_mem->get_latest(blob_id)) {
    cout << "Error: could not find blob!" << endl;
    return 2;
  }

  // ------------------------------------------------------------------
  // Downloading data
  cout << "Downloading file " << output_file << "..." << endl;
  int blob_size = my_mem->get_size();
  //int blob_size = 10485760;
  int blob_page_size = my_mem->get_page_size();
  cout << "Blob size = " << blob_size << endl;
  cout << "Blob page size = " << blob_page_size << endl;

  char* buffer = (char*)malloc(page_size*sizeof(char));
  ofstream file (output_file, ios::out|ios::binary);
  int pos = 0;
  int steps = 0;
  int count = 0;
  while (pos < blob_size) {
    if (!my_mem->read(pos, page_size, buffer))
      cout << "ERROR! Could not read (" << pos << ", " << page_size << ")" << endl;
    else {
      file.write(buffer, page_size);
      count += page_size;
    }
    pos = pos + page_size;
    steps++;
  }
  file.close();
  cout << "FINISHED: "<< pos << " bytes downloaded in blocks of " << page_size << " bytes (" << steps << " iterations)"<< endl;

  // ------------------------------------------------------------------
  // Finishing and closing stuff
  cout << "FINISHED: Blob readed with id = " << my_mem->get_id() << " and size = " << blob_size << endl;

  delete my_mem;
   
  return 0;
}
