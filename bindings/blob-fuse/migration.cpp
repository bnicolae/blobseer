#include "migration.hpp"
#include "rpc/rpc_server.hpp"

#define __DEBUG
#include "common/debug.hpp"

bool migration_wrapper::read(boost::uint64_t offset, boost::uint64_t size, char *buffer, 
			boost::uint32_t version, boost::uint32_t threshold,
			const blob::prefetch_list_t &prefetch_list) {
    boost::uint64_t index = offset / get_page_size();

    if (!remote_map.empty() && remote_map[index]) {
	bool result = true;
	buffer_wrapper wr_buffer(buffer, get_page_size(), true);
	rpcvector_t params;
	interval_range_query::replica_policy_t no_rep;
	metadata::replica_list_t replica_list;
	replica_list.push_back(metadata::provider_desc(orig_host, orig_port));
	no_rep.set_providers(buffer_wrapper(), buffer_wrapper(replica_list, true)); 

	params.push_back(buffer_wrapper(index, false));
	direct_rpc->dispatch(orig_host, orig_port, MIGR_READ, params,
			     boost::bind(&migration_wrapper::rpc_provider_callback, 
					 this, MIGR_READ, params.back(), no_rep,
					 wr_buffer, boost::ref(result), 0, _1, _2),
			     rpcvector_t(1, wr_buffer));
	direct_rpc->run();

	chunks_left--;
	DBG_COND(chunks_left == 0, "migration completed: all chunks successfully pulled");

	return result;
    } else
	return object_handler::read(offset, size, buffer, version, threshold, prefetch_list);
}

rpcreturn_t migration_wrapper::read_chunk(const rpcvector_t &params, rpcvector_t &result) {
    boost::uint64_t index;

    if (params.size() != 1 || !params[0].getValue(&index, true)) {
	ERROR("RPC error: wrong argument(s), chunk index required");
	return rpcstatus::earg;
    }
    ASSERT(index * get_page_size() < get_size());
    result.push_back(buffer_wrapper(mapping + index * get_page_size(), get_page_size(), true));
    return rpcstatus::ok;
}

rpcreturn_t migration_wrapper::accept_start(updater_t updater, const rpcvector_t &params, 
					    rpcvector_t &result, const std::string &id) {
    fill(remote_map.begin(), remote_map.end(), false);
    chunks_left = 0;
    if (params.size() != 1 || !params[0].getValue(&remote_map, true)) {
	ERROR("RPC error: wrong argument(s), chunk map required");
	return rpcstatus::earg;
    }
    int pos = id.find(":");
    orig_host = id.substr(0, pos);
    orig_port = id.substr(pos + 1, id.length());

    DBG("migration initiated by " << id);
    for (unsigned int i = 0; i < remote_map.size(); i++)
	if (remote_map[i])
	    updater(i * get_page_size());

    return rpcstatus::ok;
}

void migration_wrapper::migr_exec(updater_t updater, const std::string &service) {
    rpc_server<config::socket_namespace> migr_server(io_service);

    migr_server.register_rpc(MIGR_START,
			     (rpcserver_extcallback_t)boost::bind(
				 &migration_wrapper::accept_start, this, updater, _1, _2, _3));
    migr_server.register_rpc(MIGR_READ,
			     (rpcserver_callback_t)boost::bind(&migration_wrapper::read_chunk,
							       this, _1, _2));
    migr_server.start_listening(config::socket_namespace::endpoint(
				    config::socket_namespace::v4(),
				    atoi(service.c_str())));
    INFO(migr_server.pretty_format_str() << " ready to accept migration requests");
    io_service.run();
}

void migration_wrapper::terminate() {
    io_service.stop();
}

static void start_callback(bool &res, const rpcreturn_t &error, 
			   const rpcvector_t &result) {
    if (error == rpcstatus::ok && result.size() == 0)
	return;
    res = false;
    ERROR("could initiate migration; RPC status is: " << error);
}

bool migration_wrapper::start(const std::string &host,
			      const std::string &service,
			      char *mapping_,
			      const std::vector<bool> &written_map) {
    bool result;
    rpcvector_t params;

    mapping = mapping_;
    params.push_back(buffer_wrapper(written_map, true));
    direct_rpc->dispatch(host, service, MIGR_START, params,
			 boost::bind(&start_callback, boost::ref(result), _1, _2));
    direct_rpc->run();
    return result;
}
