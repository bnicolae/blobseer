#include <iostream>
#include <cstdio>

#include "libconfig.h++"
#include "page_manager.hpp"
#include "pmgr_listener.hpp"
#include "rpc/rpc_server.hpp"
#include "bdb_bw_map.hpp"
#include "null_bw_map.hpp"

using namespace std;

unsigned int cache_slots, total_space, rate, sync_timeout;
std::string service, phost, pservice, db_name;

template <class Persistency> void run_server() {
    boost::asio::io_service io_service;
    rpc_server<config::socket_namespace, config::lock_t> provider_server(io_service);

    page_manager<Persistency> provider_storage(db_name, cache_slots, ((boost::uint64_t)1 << 20) * total_space, sync_timeout);
    pmgr_listener plistener(io_service, phost, pservice, ((boost::uint64_t)1 << 20) * total_space, service);

    provider_storage.add_listener(boost::bind(&pmgr_listener::update_event, boost::ref(plistener), _1, _2));
    provider_server.register_rpc(PROVIDER_WRITE,
				 (rpcserver_extcallback_t)boost::bind(&page_manager<Persistency>::write_page,
								      boost::ref(provider_storage), _1, _2, _3));
    provider_server.register_rpc(PROVIDER_READ,
				 (rpcserver_extcallback_t)boost::bind(&page_manager<Persistency>::read_page,
								      boost::ref(provider_storage), _1, _2, _3));
    provider_server.register_rpc(PROVIDER_READ_PARTIAL,
				 (rpcserver_extcallback_t)boost::bind(&page_manager<Persistency>::read_partial_page,
								      boost::ref(provider_storage), _1, _2, _3));
    
    provider_server.start_listening(config::socket_namespace::endpoint(config::socket_namespace::v4(), atoi(service.c_str())));
    INFO("listening on " << provider_server.pretty_format_str() << ", offering max. " << total_space << " MB");
    io_service.run();
}

int main(int argc, char *argv[]) {   
    if (argc != 2 && argc != 3) {
	cout << "Usage: provider <config_file> [<port>]" << endl;
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("provider.service", service)
	      && cfg.lookupValue("provider.cacheslots", cache_slots) 
	      && cfg.lookupValue("pmanager.host", phost) 
	      && cfg.lookupValue("pmanager.service", pservice)
	      && cfg.lookupValue("provider.urate", rate)
	      && cfg.lookupValue("provider.dbname", db_name)
	      && cfg.lookupValue("provider.space", total_space)
	      && cfg.lookupValue("provider.sync", sync_timeout)
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
    // if port is given, override setting
    if (argc == 3)
	service = std::string(argv[2]);

    if (db_name != "")
	run_server<bdb_bw_map>();
    else
	run_server<null_bw_map>();

    return 0;
}
