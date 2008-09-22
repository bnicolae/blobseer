#include <iostream>
#include <fstream>
#include <libconfig.h++>

#include "common/config.hpp"
#include "common/debug.hpp"
#include "adv_manager.hpp"
#include "rpc/rpc_server.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    unsigned int threads;
    std::string host, service;

    if (argc != 2) {
	cout << "Usage: publisher <config_file>" << endl;
	cout << "Config file:\npublisher: {\n\thost = <host_name>\n\tservice = <service_no>\n\tthreads = <max_threads>\n};\n";
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("publisher.host", host) && cfg.lookupValue("publisher.service", service) 
	      && cfg.lookupValue("publisher.threads", threads)))
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
    adv_manager adv_storage;
    provider_server.register_rpc(PUBLISHER_UPDATE, 				 				 
				 boost::bind(&adv_manager::update, boost::ref(adv_storage), _1, _2));
    provider_server.register_rpc(PUBLISHER_GET, 
				 boost::bind(&adv_manager::get, boost::ref(adv_storage), _1, _2));
    provider_server.start_listening(host, service);
    INFO("listening on " << provider_server.getAddressRepresentation() << ", spawning max. " << threads << " threads.");
    io_service.run();
    return 0;
}
