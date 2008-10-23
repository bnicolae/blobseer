#include <iostream>
#include <fstream>
#include <cstdio>

#include "libconfig.h++"
#include "page_manager.hpp"
#include "rpc/rpc_server.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    unsigned int pages, rate;
    std::string host, service, phost, pservice;

    if (argc != 2) {
	cout << "Usage: provider <config_file>" << endl;
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("provider.service", service)
	      && cfg.lookupValue("provider.pages", pages) 
	      && cfg.lookupValue("pmanager.host", phost) 
	      && cfg.lookupValue("pmanager.service", pservice)
	      && cfg.lookupValue("provider.urate", rate)
		))
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
    
    page_manager provider_storage(io_service, provider_adv("", service, pages, rate), phost, pservice);    

    provider_server.register_rpc(PROVIDER_WRITE, 				 				 
				 (rpcserver_callback_t)boost::bind(&page_manager::write_page, boost::ref(provider_storage), _1, _2));
    provider_server.register_rpc(PROVIDER_READ, 
				 (rpcserver_callback_t)boost::bind(&page_manager::read_page, boost::ref(provider_storage), _1, _2));
    provider_server.register_rpc(PROVIDER_FREE, 
				 (rpcserver_callback_t)boost::bind(&page_manager::free_page, boost::ref(provider_storage), _1, _2));
    provider_server.start_listening(config::socket_namespace::endpoint(config::socket_namespace::v4(), atoi(service.c_str())));
    INFO("listening on " << provider_server.pretty_format_str() << ", offering max. " << pages << " mem slots");
    io_service.run();
    return 0;
}
