#include <iostream>
#include "client/object_handler.hpp"
#include "common/debug.hpp"
#include <cstdlib>

using namespace std;
using namespace boost;

const uint64_t MAX_SIZE = (uint64_t)1 << 40; // 1 TB
const uint64_t PAGE_SIZE = 1 << 16; // 64 KB
const uint64_t LIMIT = 1 << 30; // 1 GB

int main(int argc, char **argv) {
    unsigned int off, size;
    if (argc != 5 || sscanf(argv[2], "%u", &off) != 1 || sscanf(argv[3], "%u", &size) != 1) {
	cout << "Usage: multiple_readers <config_file> <offset> <size> <sync_file>" << endl;
	return 1;
    }

    libconfig::Config cfg;
    
    try {
        cfg.readFile(config_file.c_str());
	// get dht port
	std::string service;
	if (!cfg.lookupValue("dht.service", service))
	    throw std::runtime_error("object_handler::object_handler(): DHT port is missing/invalid");
	// get dht gateways
	libconfig::Setting &s = cfg.lookup("dht.gateways");
	int ng = s.getLength();
	if (!s.isList() || ng <= 0) 
	    throw std::runtime_error("object_handler::object_handler(): Gateways are missing/invalid");
	// get dht parameters
	int retry, timeout, cache_size;
	if (!cfg.lookupValue("dht.retry", retry) || 
	    !cfg.lookupValue("dht.timeout", timeout) || 
	    !cfg.lookupValue("dht.cachesize", cache_size))
	    throw std::runtime_error("object_handler::object_handler(): DHT parameters are missing/invalid");
	// build dht structure
	dht = new dht_t(io_service, retry, timeout);
	for (int i = 0; i < ng; i++) {
	    std::string stmp = s[i];
	    dht->addGateway(stmp, service);
	}
	// get other parameters
	if (!cfg.lookupValue("pmanager.host", publisher_host) ||
	    !cfg.lookupValue("pmanager.service", publisher_service) ||
	    !cfg.lookupValue("vmanager.host", lockmgr_host) ||
	    !cfg.lookupValue("vmanager.service", lockmgr_service))
	    throw std::runtime_error("object_handler::object_handler(): object_handler parameters are missing/invalid");
	// complete object creation
	query = new interval_range_query(dht);
	direct_rpc = new rpc_client_t(io_service);
    } catch(libconfig::FileIOException) {
	throw std::runtime_error("object_handler::object_handler(): I/O error trying to parse config file");
    } catch(libconfig::ParseException &e) {
	std::ostringstream ss;
	ss << "object_handler::object_handler(): Parse exception (line " << e.getLine() << "): " << e.getError();
	throw std::runtime_error(ss.str());
    } catch(std::runtime_error &e) {
	throw e;
    } catch(...) {
	throw std::runtime_error("object_handler::object_handler(): Unknown exception");
    }
    DBG("constructor init complete");


    // alloc chunk size
    char *big_zone = (char *)malloc(size); 

    object_handler *my_mem;
    my_mem = new object_handler(1, PAGE_SIZE, string(argv[1]));
    srand(time(NULL));
    
    while(access(argv[4], F_OK) != 0);

    if (my_mem->getLatestVersion()) for (int i = 0; i < 100; i++) {
	cout << "Pass " << i << " out of 100:" << endl;
	if (!my_mem->read(off, size, big_zone))
	    cout << "Pass " << i << " FAILED!" << endl;
	else
	    cout << "Pass " << i << " OK!" << endl;
    } else
	cout << "Could not initiate READ: getLatestVersion() failed" << endl;
    
    free(big_zone);
    delete my_mem;
    
    cout << "End of test" << endl;
    return 0;
}
