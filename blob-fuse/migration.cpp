#include <limits.h>
#include <boost/serialization/set.hpp>

#include "migration.hpp"
#include "rpc/rpc_server.hpp"

#define __DEBUG
#include "common/debug.hpp"

bool migration_wrapper::read(boost::uint64_t offset, boost::uint64_t size, char *buffer, 
			boost::uint32_t version, boost::uint32_t threshold,
			const blob::prefetch_list_t &prefetch_list) {
    unsigned int index = offset / get_page_size();

    if (touched.find(index) != touched.end() || untouched.find(index) != untouched.end()) {
	bool result = true;
	buffer_wrapper wr_buffer(buffer, get_page_size(), true);
	rpcvector_t params;
	interval_range_query::replica_policy_t no_rep;
	metadata::provider_list_t provider_list;
	provider_list.push_back(metadata::provider_desc(source_host, source_port));
	no_rep.set_providers(buffer_wrapper(), buffer_wrapper(provider_list, true)); 

	params.push_back(buffer_wrapper(index, false));
	direct_rpc->dispatch(source_host, source_port, MIGR_READ, params,
			     boost::bind(&migration_wrapper::rpc_result_callback, 
					 this, boost::ref(result), _1, _2),
			     rpcvector_t(1, wr_buffer));
	direct_rpc->run();

	boost::interprocess::detail::atomic_dec32(&chunks_left);
	DBG_COND(boost::interprocess::detail::atomic_read32(&chunks_left) == 0, 
		 "migration completed: all chunks successfully pulled");

	return result;
    } else
	return object_handler::read(offset, size, buffer, version, threshold, prefetch_list);
}

rpcreturn_t migration_wrapper::read_chunk(const rpcvector_t &params, rpcvector_t &result) {
    unsigned int index;

    if (params.size() != 1 || !params[0].getValue(&index, false)) {
	ERROR("RPC error: wrong argument(s), chunk index required");
	return rpcstatus::earg;
    }
    ASSERT(index * get_page_size() < get_size());
    result.push_back(buffer_wrapper(mapping + index * get_page_size(), get_page_size(), true));
    return rpcstatus::ok;
}

bool migration_wrapper::push_chunk(unsigned int index, char *content) {
    bool result = true;

    rpcvector_t params;
    params.push_back(buffer_wrapper(index, false));
    params.push_back(buffer_wrapper(content, get_page_size(), true));
    direct_rpc->dispatch(target_host, target_port, MIGR_PUSH, params,
			 boost::bind(&migration_wrapper::rpc_result_callback, this, 
				     boost::ref(result), _1, _2));
    direct_rpc->run();
    return result;
}

rpcreturn_t migration_wrapper::accept_chunk(updater_t cancel_chunk, const rpcvector_t &params, 
					    rpcvector_t &result) {
    unsigned int index;

    if (params.size() != 2 || 
	!params[0].getValue(&index, false) ||
	params[1].size() != get_page_size()) {
	ERROR("RPC error: wrong argument(s), chunk index and content required");
	return rpcstatus::earg;
    }
    ASSERT(index * get_page_size() < get_size());
    cancel_chunk(index);
    memcpy(mapping + index * get_page_size(), params[1].get(), get_page_size());

    return rpcstatus::ok;
}

rpcreturn_t migration_wrapper::receive_control(updater_t updater, const rpcvector_t &params, 
					       rpcvector_t &result, const std::string &id) {
    if (params.size() != 2 || 
	!params[0].getValue(&touched, true) || 
	!params[1].getValue(&untouched, true)) {
	ERROR("RPC error: wrong argument(s), touched/untouched required");
	return rpcstatus::earg;
    }
    int pos = id.find(":");
    source_host = id.substr(0, pos);

    unsigned int pending = 0;
    for (migr_set_t::iterator it = touched.begin(); it != touched.end(); it++)
	if (!updater((*it) * get_page_size()))
	    pending++;
    for (migr_set_t::iterator it = untouched.begin(); it != untouched.end(); it++)
	if (!updater((*it) * get_page_size()))
	    pending++;

    boost::interprocess::detail::atomic_write32(&chunks_left, 
						touched.size() + untouched.size() - pending);
    DBG("migration initiated by " << id << ", touched/untouched = " 
	<< touched.size() << "/" << untouched.size());

    return rpcstatus::ok;
}

void migration_wrapper::migr_exec(updater_t updater, updater_t cancel_chunk,
				  const std::string &service) {
    boost::asio::io_service local_service;
    rpc_server<config::socket_namespace> migr_server(local_service);

    migr_server.register_rpc(MIGR_PUSH,
			     (rpcserver_callback_t)boost::bind(
				 &migration_wrapper::accept_chunk, this, cancel_chunk, 
				 _1, _2));
    migr_server.register_rpc(MIGR_TRANSFER,
			     (rpcserver_extcallback_t)boost::bind(
				 &migration_wrapper::receive_control, this, updater,
				 _1, _2, _3));
    migr_server.register_rpc(MIGR_READ,
			     (rpcserver_callback_t)boost::bind(
				 &migration_wrapper::read_chunk, this, _1, _2));
    
    source_port = service;
    io_service = &local_service;
    migr_server.start_listening(config::socket_namespace::endpoint(
				    config::socket_namespace::v4(),
				    atoi(service.c_str())));
    INFO(migr_server.pretty_format_str() << " ready to accept migration requests");
    local_service.run();
}

void migration_wrapper::terminate() {
    io_service->stop();
}

static void start_callback(bool &res, const rpcreturn_t &error, 
			   const rpcvector_t &result) {
    if (error == rpcstatus::ok && result.size() == 0)
	return;
    res = false;
    ERROR("could initiate migration; RPC status is: " << error);
}

void migration_wrapper::start(const std::string &host,
			      const std::string &service,
			      const std::vector<bool> &written_map) {
    target_host = host;
    target_port = service;
    touched.clear();
    untouched.clear();
    for (unsigned int i = 0; i < written_map.size(); i++)
	if (written_map[i])
	    untouched.insert(i);
}

void migration_wrapper::touch(unsigned int idx) {
    boost::mutex::scoped_lock lock(touch_lock);

    migr_set_t::iterator it = untouched.find(idx);
    if (it == untouched.end()) {
	it = touched.find(idx);
	if (it == touched.end())
	    untouched.insert(idx);
    } else {
	untouched.erase(it);
	touched.insert(idx);
    }
}

void migration_wrapper::untouch(unsigned int idx) {
    boost::mutex::scoped_lock lock(touch_lock);

    untouched.erase(idx);
    touched.erase(idx);
}

int migration_wrapper::next_chunk_to_push() {
    boost::mutex::scoped_lock lock(touch_lock);

    if (untouched.empty())
	return UINT_MAX;
    migr_set_t::iterator it = untouched.begin();
    unsigned int ret = *it;
    untouched.erase(it);

    return ret;
}

bool migration_wrapper::transfer_control() {
    bool result;
    rpcvector_t params;

    params.push_back(buffer_wrapper(touched, true));
    params.push_back(buffer_wrapper(untouched, true));
    direct_rpc->dispatch(target_host, target_port, MIGR_TRANSFER, params,
			 boost::bind(&start_callback, boost::ref(result), _1, _2));
    direct_rpc->run();
    return result;
}
