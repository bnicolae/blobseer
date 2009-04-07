#include <iostream>

#include "libconfig.h++"
#include "page_manager.hpp"
#include "rpc/rpc_server.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    unsigned int pages;
    std::string service;

    if (argc != 2 && argc != 3) {
	cout << "Usage: sdht <config_file> [<port>]" << endl;
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("dht.service", service)
	      && cfg.lookupValue("sdht.maxhash", pages)))
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
    // if port is given, override setting
    if (argc == 3)
	service = std::string(argv[2]);

    boost::asio::io_service io_service;
    rpc_server<config::socket_namespace, config::lock_t> provider_server(io_service);
    
    page_manager provider_storage(pages);

    provider_server.register_rpc(PROVIDER_WRITE,
				 (rpcserver_extcallback_t)boost::bind(&page_manager::write_page, boost::ref(provider_storage), _1, _2, _3));
    provider_server.register_rpc(PROVIDER_READ,
				 (rpcserver_extcallback_t)boost::bind(&page_manager::read_page, boost::ref(provider_storage), _1, _2, _3));

    provider_server.start_listening(config::socket_namespace::endpoint(config::socket_namespace::v4(), atoi(service.c_str())));
    INFO("listening on " << provider_server.pretty_format_str() << ", offering max. " << pages << " mem slots");
    io_service.run();
    return 0;
}
