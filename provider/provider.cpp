#include <iostream>
#include <fstream>
#include <cstdio>

#include "libconfig.h++"
#include "page_manager.hpp"
#include "rpc/rpc_server.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    unsigned int pages, threads, rate;
    std::string host, service, phost, pservice;

    if (argc != 2) {
	cout << "Usage: provider <config_file>" << endl;
	cout << "Config file:\nprovider: {\n\thost = <host_ip>;\n\tservice = <service_no>;\n\tthreads = <max_threads>;\n\tpages = <no_pages>;\n";
	cout << "\tphost = <publisher_host>;\n\tpservice = <publisher_service>;\n\turate = <update_rate>;\n};\n";
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("provider.host", host) && cfg.lookupValue("provider.service", service)
	      && cfg.lookupValue("provider.threads", threads) && cfg.lookupValue("provider.pages", pages) 
	      && cfg.lookupValue("provider.phost", phost) && cfg.lookupValue("provider.pservice", pservice)
	      && cfg.lookupValue("provider.urate", rate)))
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
    rpc_server<config::socket_namespace, config::lock_t> provider_server(io_service);
    page_manager provider_storage(io_service, provider_adv(host, service, pages, rate), phost, pservice);    

    provider_server.register_rpc(PROVIDER_WRITE, 				 				 
				 boost::bind(&page_manager::write_page, boost::ref(provider_storage), _1, _2));
    provider_server.register_rpc(PROVIDER_READ, 
				 boost::bind(&page_manager::read_page, boost::ref(provider_storage), _1, _2));
    provider_server.register_rpc(PROVIDER_FREE, 
				 boost::bind(&page_manager::free_page, boost::ref(provider_storage), _1, _2));
    INFO("listening on " << provider_server.getAddressRepresentation() << ", spawning max. " << threads 
	 << " threads, offering max. " << pages << " mem slots");
    provider_server.start_listening(host, service);
    io_service.run();
    return 0;
}
