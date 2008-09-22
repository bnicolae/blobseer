#include <sstream>
#include <iostream>

#include "libconfig.h++"
#include "page_manager.hpp"
#include "rpc/rpc_server.hpp"
#include "sdht.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    unsigned int pages, threads, port;
    std::string host, service, phost, pservice;

    if (argc != 2) {
	cout << "Usage: sdht <config_file>" << endl;
	cout << "Config file:\nprovider: {\n\thost = <host_ip>;\n\tservice = <service_no>;\n\tthreads = <max_threads>;\n\tpages = <no_pages>;\n";
	cout << "\tphost = <publisher_host>;\n\tpservice = <publisher_service>;\n};\n";
	cout << "Actual port used is (service + 1) and pages is reset to 2^30." << endl;
	cout << "This allows single config both for provider and sdht" << endl;
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("provider.host", host) && cfg.lookupValue("provider.service", service)
	      && cfg.lookupValue("provider.threads", threads) && cfg.lookupValue("provider.pages", pages) 
	      && cfg.lookupValue("provider.phost", phost) && cfg.lookupValue("provider.pservice", pservice)))
	    throw libconfig::ConfigException();
	istringstream is(service);
	is >> port;
	ostringstream os;
	os << (port + 1);
	service = os.str();
	pages = 1 << 30;
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
    rpc_server<config::socket_namespace, config::lock_t> sdht_server(io_service);
    page_manager sdht_storage(pages);

    sdht_server.register_rpc(SIMPLEDHT_WRITE, 
			     boost::bind(&page_manager::write_page, boost::ref(sdht_storage), _1, _2));
    sdht_server.register_rpc(SIMPLEDHT_READ, 
			     boost::bind(&page_manager::read_page, boost::ref(sdht_storage), _1, _2));
    INFO("listening on " << sdht_server.getAddressRepresentation() << ", spawning max. " << threads 
	 << " threads, offering max. " << pages << " mem slots");
    sdht_server.start_listening(host, service);
    io_service.run();
    return 0;
}
