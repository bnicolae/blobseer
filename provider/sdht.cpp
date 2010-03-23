#include <iostream>

#include "libconfig.h++"
#include "page_manager.hpp"
#include "rpc/rpc_server.hpp"
#include "bdb_bw_map.hpp"
#include "null_bw_map.hpp"

using namespace std;

unsigned int cache_slots, total_space;
bool compressed;
std::string service, db_name;

template <class Storage> void run_server(Storage &provider_storage) {
    boost::asio::io_service io_service;
    rpc_server<config::socket_namespace> provider_server(io_service);

    provider_server.register_rpc(PROVIDER_WRITE,
				 (rpcserver_extcallback_t)boost::bind(&Storage::write_page, 
								      boost::ref(provider_storage), _1, _2, _3));
    provider_server.register_rpc(PROVIDER_READ,
				 (rpcserver_extcallback_t)boost::bind(&Storage::read_page, 
								      boost::ref(provider_storage), _1, _2, _3));
    provider_server.start_listening(config::socket_namespace::endpoint(config::socket_namespace::v4(), atoi(service.c_str())));
    INFO("listening on " << provider_server.pretty_format_str() << ", offering max. " << total_space << " MB");
    io_service.run();
}

int main(int argc, char *argv[]) {   
    

    if (argc != 2 && argc != 3) {
	cout << "Usage: sdht <config_file> [<port>]" << endl;
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("dht.service", service)
	      && cfg.lookupValue("sdht.cacheslots", cache_slots)
	      && cfg.lookupValue("sdht.dbname", db_name)
	      && cfg.lookupValue("sdht.space", total_space)
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
	
    if (db_name != "") {
	bdb_bw_map provider_map(db_name, cache_slots, ((boost::uint64_t)1 << 20) * total_space);
	page_manager<bdb_bw_map> provider_storage(&provider_map, false);
	run_server<page_manager<bdb_bw_map> >(provider_storage);
    } else {
	null_bw_map provider_map(cache_slots, ((boost::uint64_t)1 << 20) * total_space);
	page_manager<null_bw_map> provider_storage(&provider_map, false);
	run_server<page_manager<null_bw_map> >(provider_storage);
    }

    return 0;
}
