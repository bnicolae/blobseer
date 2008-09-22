#include <iostream>
#include <fstream>

#include <boost/bind.hpp>

#include "libconfig.h++"
#include "lock_management.hpp"
#include "rpc/rpc_server.hpp"
#include "lockmgr.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    unsigned int threads;
    std::string host, service;

    if (argc != 2) {
	cout << "Usage: " << argv[0] << " <config_file>" << endl;
	cout << "Config file:\nlockmgr: {\n\thost = <host_name>\n\tservice = <service_no>\n\tthreads = <max_threads>\n};\n";
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("lockmgr.host", host) && cfg.lookupValue("lockmgr.service", service) 
	      && cfg.lookupValue("lockmgr.threads", threads)))
	    throw libconfig::ConfigException();
    } catch(libconfig::FileIOException &e) {
	ERROR("I/O exception on config file");
	return 2;
    } catch(libconfig::ParseException &e) {
	ERROR("Parse exception (line " << e.getLine() << "): " << e.getError());
	return 3;
    } catch(...) {
	ERROR("Invalid configuration, check format (run without config file for help)");
	return 4;
    }

    boost::asio::io_service io_service;
    rpc_server<config::socket_namespace, config::lock_t> lockmgr_server(io_service);
    lock_management lock;
    lockmgr_server.register_rpc(LOCKMGR_GETTICKET,
				boost::bind(&lock_management::getTicket, boost::ref(lock), _1, _2));
    lockmgr_server.register_rpc(LOCKMGR_CREATE,
				boost::bind(&lock_management::create, boost::ref(lock), _1, _2));
    lockmgr_server.register_rpc(LOCKMGR_LASTVER,
				boost::bind(&lock_management::getVersion, boost::ref(lock), _1, _2));
    lockmgr_server.register_rpc(LOCKMGR_GETRANGEVER,
				boost::bind(&lock_management::getIntervalVersion, boost::ref(lock), _1, _2));
    lockmgr_server.register_rpc(LOCKMGR_PUBLISH,
				boost::bind(&lock_management::publish, boost::ref(lock), _1, _2));
    lockmgr_server.register_rpc(LOCKMGR_GETOBJNO,
				boost::bind(&lock_management::get_objcount, boost::ref(lock), _1, _2));

    INFO("Listening on " << lockmgr_server.getAddressRepresentation());
    lockmgr_server.start_listening(host, service);
    io_service.run();
    return 0;
}
