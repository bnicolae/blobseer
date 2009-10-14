#include <iostream>
#include <fstream>
#include <libconfig.h++>

#include "common/config.hpp"
#include "common/debug.hpp"
#include "adv_manager.hpp"
#include "rpc/rpc_server.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    std::string host, service;

    if (argc != 2) {
	cout << "Usage: " << argv[0] << " <config_file>" << endl;
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("pmanager.service", service)/* && cfg.lookupValue("pmanager.threads", threads)*/))
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
    rpc_server<config::socket_namespace> provider_server(io_service);
    adv_manager adv_storage;

    provider_server.register_rpc(PUBLISHER_UPDATE,
				 (rpcserver_extcallback_t)boost::bind(&adv_manager::update, boost::ref(adv_storage), _1, _2, _3));
    provider_server.register_rpc(PUBLISHER_GET,
				 (rpcserver_callback_t)boost::bind(&adv_manager::get, boost::ref(adv_storage), _1, _2));
    provider_server.start_listening(config::socket_namespace::endpoint(config::socket_namespace::v4(), atoi(service.c_str())));
    INFO("listening on " << provider_server.pretty_format_str());
    io_service.run();
    return 0;
}
