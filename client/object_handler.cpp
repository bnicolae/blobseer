/**
 * Copyright (C) 2008-2012 Bogdan Nicolae <bogdan.nicolae@inria.fr>
 *
 * This file is part of BlobSeer, a scalable distributed big data
 * store. Please see README (root of repository) for more details.
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses>.
 */

#include <sstream>
#include <libconfig.h++>
#include <openssl/md5.h>

#include "pmanager/publisher.hpp"
#include "vmanager/main.hpp"
#include "object_handler.hpp"
#include "replica_policy.hpp"

#include "ft_replication.hpp"
#include "ft_erasure.hpp"

#include "common/debug.hpp"

using namespace std;

#define HASH_FCN   MD5
const unsigned int HASH_SIZE = 16;

object_handler::object_handler(const std::string &config_file) : latest_root(0, 0, 0, 0, 0) {
    libconfig::Config cfg;
    std::string engine_type;
    
    try {
        cfg.readFile(config_file.c_str());
	// get dht port
	std::string service;
	if (!cfg.lookupValue("dht.service", service))
	    FATAL("DHT port is missing/invalid");
	// get dht gateways
	libconfig::Setting &s = cfg.lookup("dht.gateways");
	int ng = s.getLength();
	if (!s.isList() || ng <= 0) 
	    FATAL("Gateways are missing/invalid");
	// get dht parameters
	int replication, timeout, cache_size;
	if (!cfg.lookupValue("dht.replication", replication) ||
	    !cfg.lookupValue("dht.timeout", timeout) ||
	    !cfg.lookupValue("dht.cachesize", cache_size))
	    FATAL("DHT parameters are missing/invalid");
	// build dht structure
	dht = new dht_t(io_service, replication, timeout, cache_size);
	for (int i = 0; i < ng; i++) {
	    std::string stmp = s[i];
	    dht->addGateway(stmp, service);
	}
	// get provider parameters
	if (!cfg.lookupValue("provider.retry", retry_count))
	    FATAL("Provider retry count is missing/invalid");
	if (!cfg.lookupValue("provider.deduplication", dedup_flag))
	    FATAL("Provider deduplication flag is missing/invalid");
	if (!cfg.lookupValue("provider.ft_engine", engine_type))
	    FATAL("Provider fault tolerance engine is missing/invalid");

	// get other parameters
	if (!cfg.lookupValue("pmanager.host", publisher_host) ||
	    !cfg.lookupValue("pmanager.service", publisher_service) ||
	    !cfg.lookupValue("vmanager.host", vmgr_host) ||
	    !cfg.lookupValue("vmanager.service", vmgr_service))
	    FATAL("pmanager/vmanager host/service are missing/invalid");
	// complete object creation
	query = new interval_range_query(dht);
	direct_rpc = new rpc_client_t(io_service);	
	if (engine_type == "replication")
	    ft_engine = new ft_replication_t(direct_rpc, retry_count);
	else if (engine_type == "erasure")
	    ft_engine = new ft_erasure_t(direct_rpc, retry_count);
	else 
	    FATAL("Supplied fault tolerance engine (" + engine_type + ") is not implemented");
	    
    } catch(libconfig::FileIOException) {
	FATAL("I/O error trying to parse config file: " + config_file);
    } catch(libconfig::ParseException &e) {
	std::ostringstream ss;
	ss << "parse exception for cfg file " << config_file 
	   << "(line " << e.getLine() << "): " << e.getError();
	FATAL(ss.str());
    } catch(std::runtime_error &e) {
	throw e;
    } catch(...) {
	FATAL("unexpected exception");
    }

    // set the random number generator seed
    rnd.seed(boost::posix_time::microsec_clock::local_time().time_of_day().total_nanoseconds());
        
    DBG("constructor init complete");
}

object_handler::~object_handler() {
    delete direct_rpc;
    delete query;
    delete dht;
    delete ft_engine;
}


void object_handler::rpc_pagekey_callback(std::vector<bool> &leaf_duplication_flag,
					  unsigned int index,
					  buffer_wrapper key,
					  buffer_wrapper providers) {
    leaf_duplication_flag[index] = !providers.empty();
    if (leaf_duplication_flag[index])
	DBG("a copy page " << key << " already exists; supressing duplicate");
}
    
void object_handler::rpc_result_callback(bool &res, const rpcreturn_t &error, 
					 const rpcvector_t &) {
    if (error != rpcstatus::ok) {
	res = false;
	ERROR("could not perform RPC successfully, RPC status is: " << error);
    }
}

bool object_handler::get_root(boost::uint32_t version, metadata::root_t &root) {
    bool result = true;

    if (version == 0 || !version_cache.read(version, &root)) {
	rpcvector_t params;
	params.push_back(buffer_wrapper(id, true));
	params.push_back(buffer_wrapper(version, true));
	direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_GETROOT, params,
			     bind(&rpc_client_t::rpc_get_serialized<metadata::root_t>, 
				  boost::ref(result), 
				  boost::ref(root), _1, _2));
	direct_rpc->run();
	if (result && version != 0)
	    version_cache.write(root.node.version, root);
    }

    return result;
}

bool object_handler::get_locations(page_locations_t &loc, boost::uint64_t offset, 
				   boost::uint64_t size, boost::uint32_t version) {
    metadata::root_t query_root(0, 0, 0, 0, 0);

    if (version == 0)
	query_root = latest_root;
    else
	if (!get_root(version, query_root))
	    return false;

    DBG("query root = " << query_root.node <<  ", version = " << version);

    if (query_root.node.version < version)
	throw std::runtime_error("object_handler::get_locations(): requested"
				 " version higher than latest available version");
    if (query_root.page_size == 0)
	throw std::runtime_error("object_handler::get_locations(): read attempt"
				 " on unallocated/uninitialized object");
    if (offset + size > query_root.node.size)
	throw std::runtime_error("object_handler::get_locations(): read attempt"
				 " beyond maximal size");
    
    TIMER_START(read_locations);
    boost::uint64_t psize = query_root.page_size;
    boost::uint64_t new_offset = (offset / psize) * psize;
    boost::uint64_t nbr_vadv = (size + offset - new_offset) / psize + 
	(((offset + size) % psize == 0) ? 0 : 1);
    boost::uint64_t new_size = nbr_vadv * psize;
    std::vector<random_select> vadv(nbr_vadv);

    metadata::query_t range(query_root.node.id, query_root.node.version, new_offset, new_size);
    blob::prefetch_list_t unused;

    TIMER_START(meta_timer);
    bool result = query->readRecordLocations(vadv, unused, range, query_root, 0xFFFFFFFF);
    TIMER_STOP(meta_timer, "GET_LOCATIONS " << range 
	       << ": Metadata read operation, success: " << result);
    if (!result)
	return false;

    metadata::provider_desc adv;
    for (boost::uint64_t i = 0; i < vadv.size(); i++)
	while (1) {
	    metadata::provider_desc adv = vadv[i].try_next();
	    if (adv.empty()) 
		break;
	    loc.push_back(page_location_t(adv, i * query_root.page_size, query_root.page_size));
	}
    TIMER_STOP(read_locations, "GET_LOCATIONS " << range 
	       << ": Page location vector has been successfully constructed");

    return true;
}

bool object_handler::read(boost::uint64_t offset, boost::uint64_t size, char *buffer, 
			  boost::uint32_t version, boost::uint32_t threshold,
			  const blob::prefetch_list_t &prefetch_list) {
    metadata::root_t query_root(0, 0, 0, 0, 0);

    if (version == 0)
	query_root = latest_root;
    else
	if (!get_root(version, query_root))
	    return false;

    DBG("query root = " << query_root.node <<  ", version = " << version);

    if (query_root.node.version < version)
	throw std::runtime_error("object_handler::read(): read attempt"
				 " on a version higher than latest available version");
    if (query_root.page_size == 0)
	throw std::runtime_error("object_handler::read(): read attempt on"
				 " unallocated/uninitialized object");
    if (offset + size > query_root.node.size)
	throw std::runtime_error("object_handler::read(): read attempt"
				 " beyond maximal size");

    TIMER_START(read_timer);
    boost::uint64_t psize = query_root.page_size;
    boost::uint64_t new_offset = (offset / psize) * psize;
    boost::uint64_t nbr_vadv = (size + offset - new_offset) / psize + 
	(((offset + size) % psize == 0) ? 0 : 1);
    boost::uint64_t new_size = nbr_vadv * psize;
    std::vector<random_select> vadv(nbr_vadv);

    metadata::query_t range(query_root.node.id, query_root.node.version, new_offset, new_size);

    TIMER_START(meta_timer);
    // !! BAD practice to const_cast. Interface needs to be redesigned.
    bool result = query->readRecordLocations(vadv, 
					     const_cast<blob::prefetch_list_t &>(prefetch_list), 
					     range, query_root, threshold);
    TIMER_STOP(meta_timer, "READ " << range << ": Metadata read operation, success: " << result);
    if (!result)
	return false;

    rpcvector_t read_params;
    TIMER_START(data_timer);
    
    // size of partial read from leftmost/rightmost page when read operation is unaligned
    uint64_t left_part = (query_root.page_size - (offset % query_root.page_size)) 
	% query_root.page_size;
    uint64_t right_part = (offset + size) % query_root.page_size;

    buffer_wrapper left_buffer, right_buffer;

    // if the left end of range is unaligned, read only the needed part of the page involved
    unsigned int l = 0;
    if (offset % psize != 0) {
	l++;
	DBG("UNALIGNED LEFT READ QUERY " << vadv[0].get_key());
	if (left_part < size)
	    left_buffer = buffer_wrapper(buffer, left_part, true);
	else
	    left_buffer = buffer_wrapper(buffer, size, true);

	ft_engine->enqueue_read_chunk(vadv[0], psize - left_part, left_buffer, result);
	if (!result)
	    return false;
    }

    // if the right end of range is unaligned, read only the needed part of the page involved
    unsigned int r = vadv.size();
    if (((offset + size) % psize != 0 && r > 1) || (offset % psize == 0 && size < psize)) {
	r--;
	DBG("UNALIGNED RIGHT READ QUERY " << vadv[r].get_key());
	right_buffer = buffer_wrapper(buffer + left_part + (r - l) * psize, right_part, true);

	ft_engine->enqueue_read_chunk(vadv[r], 0, right_buffer, result);
	if (!result)
	    return false;
    }

    // read all aligned pages directly in the user-supplied buffer
    for (unsigned int i = l; result && i < r; i++) {
	DBG("FULL READ QUERY " << vadv[i].get_key());
	buffer_wrapper wr_buffer(buffer + left_part + (i - l) * query_root.page_size, 
				 query_root.page_size, true);

	ft_engine->enqueue_read_chunk(vadv[i], 0, wr_buffer, result);
	if (!result)
	    return false;
    }
    direct_rpc->run();
    TIMER_STOP(data_timer, "READ " << range << ": Data read operation, result: " << result);

    TIMER_STOP(read_timer, "READ " << range << ": has completed, result: " << result);
    return result;
}

boost::uint32_t object_handler::append(boost::uint64_t size, char *buffer) {
    return exec_write(0, size, buffer, true);
}

boost::uint32_t object_handler::write(boost::uint64_t offset, boost::uint64_t size, 
				      char *buffer) {
    return exec_write(offset, size, buffer, false);
}

boost::uint32_t object_handler::exec_write(boost::uint64_t offset, boost::uint64_t size, 
					   char *buffer, bool append) {
    if (latest_root.page_size == 0)
	throw std::runtime_error("object_handler::write(): write attempt"
				 " on unallocated/uninitialized object");
    ASSERT(offset % latest_root.page_size == 0 && size % latest_root.page_size == 0);
    bool result = true;
    metadata::query_t range(id, rnd(), offset, size);

    metadata::provider_list_t adv;
    std::vector<buffer_wrapper> leaf_keys;
    std::vector<bool> leaf_duplication_flag;
    std::set<buffer_wrapper> unique_keys;
    rpcvector_t params;

    // initialize the write operation
    TIMER_START(write_timer);

    // de-duplication
    TIMER_START(dedup_timer);
    for (boost::uint64_t i = 0; i * latest_root.page_size < size; i++) {
	char *hash = new char[HASH_SIZE];
	HASH_FCN((unsigned char *)buffer + i * latest_root.page_size, 
		 latest_root.page_size, (unsigned char *)hash);
	buffer_wrapper page_key(hash, HASH_SIZE);

	leaf_duplication_flag.push_back(false);
	if (dedup_flag) {
	    std::set<buffer_wrapper>::iterator it = unique_keys.find(page_key);
	    if (it == unique_keys.end()) {
		dht->get(page_key,
			 boost::bind(&object_handler::rpc_pagekey_callback, this,
				     boost::ref(leaf_duplication_flag), i, page_key, _1));
		if (!leaf_duplication_flag[i])
		    unique_keys.insert(page_key);
	    } else {
		page_key = *it;
		rpc_pagekey_callback(leaf_duplication_flag, i, page_key, page_key);
	    }
	}
	leaf_keys.push_back(page_key);
    }
    dht->wait();
    TIMER_STOP(dedup_timer, "WRITE " << range << ": de-duplication completed, "
	       << (dedup_flag ? unique_keys.size() : leaf_keys.size()) 
	       << "/" << leaf_keys.size() << " chunks need to be stored");

    // get a list of providers 
    unsigned int no_chunks = dedup_flag ? unique_keys.size() : leaf_keys.size();
    unsigned int no_groups = ft_engine->get_groups(no_chunks);
    unsigned int group_size = ft_engine->get_group_size(no_chunks);
    
    DBG("no groups = " << no_groups << ", group_size = " << group_size);
    TIMER_START(publisher_timer);
    params.clear();
    params.push_back(buffer_wrapper(no_groups * group_size, true));
    params.push_back(buffer_wrapper(group_size, true));
    direct_rpc->dispatch(publisher_host, publisher_service, PUBLISHER_GET, params,
			 boost::bind(&rpc_client_t::rpc_get_serialized<metadata::provider_list_t>,
				     boost::ref(result), 
				     boost::ref(adv), _1, _2));
    direct_rpc->run();
    TIMER_STOP(publisher_timer, "WRITE " << range << ": PUBLISHER_GET, result: " << result);
    if (!result || adv.size() < no_groups * group_size)
	return 0;

    // write the chunks to the page providers
    std::vector<buffer_wrapper> contents;
    for (unsigned int i = 0; i < leaf_duplication_flag.size(); i++)
	if (!leaf_duplication_flag[i])
	    contents.push_back(buffer_wrapper(buffer + i * latest_root.page_size, 
					      latest_root.page_size, true));
    if (dedup_flag) {
	std::vector<buffer_wrapper> key_vector(unique_keys.begin(), unique_keys.end());
	result = ft_engine->write_chunks(range, adv, key_vector, contents);
    } else
	result = ft_engine->write_chunks(range, adv, leaf_keys, contents);

    if (!result)
	return 0;
    
    // get a ticket from the version manager
    params.clear();
    params.push_back(buffer_wrapper(range, true));
    params.push_back(buffer_wrapper(append, true));

    vmgr_reply mgr_reply;
    TIMER_START(ticket_timer);
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_GETTICKET, params,
			 bind(&rpc_client_t::rpc_get_serialized<vmgr_reply>, boost::ref(result),
			      boost::ref(mgr_reply), _1, _2));
    direct_rpc->run();
    TIMER_STOP(ticket_timer, "WRITE " << range << ": VMGR_GETTICKET, result: " << result);
    if (!result)
	return 0;

    // construct the set of leaves to be written to the metadata
    range = mgr_reply.intervals.rbegin()->first;
    TIMER_START(metadata_timer);
    result = query->writeRecordLocations(mgr_reply, leaf_keys, leaf_duplication_flag, 
					 group_size, adv);
    TIMER_STOP(metadata_timer, "WRITE " << range << ": writeRecordLocations(), result: " 
	       << result);
    if (!result)
	return 0;
    
    // publish the latest written version
    TIMER_START(publish_timer);
    params.clear();
    params.push_back(buffer_wrapper(range, true));
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_PUBLISH, params,
			 boost::bind(&object_handler::rpc_result_callback, this,
				     boost::ref(result), _1, _2));
    direct_rpc->run();
    TIMER_STOP(publish_timer, "WRITE " << range << ": VMGR_PUBLISH, result: " << result);
    TIMER_STOP(write_timer, "WRITE " << range << ": has completed, result: " << result);
    
    return result ? range.version : 0;
}

bool object_handler::create(boost::uint64_t page_size, boost::uint32_t ft_info) {
    bool result = true;

    rpcvector_t params;
    params.push_back(buffer_wrapper(page_size, true));
    params.push_back(buffer_wrapper(ft_info, true));
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_CREATE, params,
			 boost::bind(rpc_client_t::rpc_get_serialized<metadata::root_t>, 
				     boost::ref(result), 
				     boost::ref(latest_root), _1, _2));
    direct_rpc->run();
    id = latest_root.node.id;
    ft_engine->set_params(page_size, ft_info);
    DBG("create result = " << result << ", id = " << id);
    return result;
}

bool object_handler::get_latest(boost::uint32_t id_) {
    if (id_ != 0)
	id = id_;

    bool result = get_root(0, latest_root);
    ft_engine->set_params(latest_root.page_size, latest_root.ft_info);
    DBG("latest version request: " << latest_root.node);
    return result;
}

boost::int32_t object_handler::get_objcount() const {
    bool result = true;
    boost::int32_t obj_no;

    rpcvector_t params;    
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_GETOBJNO, rpcvector_t(), 
			 boost::bind(rpc_client_t::rpc_get_serialized<boost::int32_t>, 
				     boost::ref(result), 
				     boost::ref(obj_no), _1, _2));
    direct_rpc->run();
    DBG("the total number of blobs is: " << obj_no);
    if (result)
	return obj_no;
    else
	return -1;
}

bool object_handler::clone(boost::int32_t id_, boost::int32_t version_) {
    metadata::root_t clone_root(0, 0, 0, 0, 0);
    bool result = true;
    if (id_ == 0)
	id_ = id;

    rpcvector_t params;
    params.push_back(buffer_wrapper(id_, true));
    params.push_back(buffer_wrapper(version_, true));
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_CLONE, params, 
			 boost::bind(rpc_client<config::socket_namespace>::rpc_get_serialized<
					 metadata::root_t>, boost::ref(result), 
				     boost::ref(clone_root), _1, _2));
    direct_rpc->run();
    DBG("new clone: " << clone_root.node);
    if (result && !clone_root.empty()) {
	latest_root = clone_root;
	latest_root.node.id = id;
	id = clone_root.node.id;
	
	return true;
    } else
	return false;
}
