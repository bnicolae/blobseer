#include <sstream>
#include <libconfig.h++>

#include <boost/dynamic_bitset.hpp>

#include "pmanager/publisher.hpp"
#include "provider/provider.hpp"
#include "vmanager/main.hpp"
#include "object_handler.hpp"
#include "replica_policy.hpp"

#include "common/debug.hpp"

using namespace std;

object_handler::object_handler(const std::string &config_file) : latest_root(0, 0, 0, 0, 0) {
    libconfig::Config cfg;
    
    try {
        cfg.readFile(config_file.c_str());
	// get dht port
	std::string service;
	if (!cfg.lookupValue("dht.service", service))
	    throw std::runtime_error("object_handler::object_handler(): DHT port is missing/invalid");
	// get dht gateways
	libconfig::Setting &s = cfg.lookup("dht.gateways");
	int ng = s.getLength();
	if (!s.isList() || ng <= 0) 
	    throw std::runtime_error("object_handler::object_handler(): Gateways are missing/invalid");
	// get dht parameters
	int retry, timeout, cache_size;
	if (!cfg.lookupValue("dht.retry", retry) || 
	    !cfg.lookupValue("dht.timeout", timeout) || 
	    !cfg.lookupValue("dht.cachesize", cache_size))
	    throw std::runtime_error("object_handler::object_handler(): DHT parameters are missing/invalid");
	// build dht structure
	dht = new dht_t(io_service, retry, timeout);
	for (int i = 0; i < ng; i++) {
	    std::string stmp = s[i];
	    dht->addGateway(stmp, service);
	}
	// get other parameters
	if (!cfg.lookupValue("pmanager.host", publisher_host) ||
	    !cfg.lookupValue("pmanager.service", publisher_service) ||
	    !cfg.lookupValue("vmanager.host", vmgr_host) ||
	    !cfg.lookupValue("vmanager.service", vmgr_service))
	    throw std::runtime_error("object_handler::object_handler(): object_handler parameters are missing/invalid");
	// complete object creation
	query = new interval_range_query(dht);
	direct_rpc = new rpc_client_t(io_service);
    } catch(libconfig::FileIOException) {
	throw std::runtime_error("object_handler::object_handler(): I/O error trying to parse config file: " + config_file);
    } catch(libconfig::ParseException &e) {
	std::ostringstream ss;
	ss << "object_handler::object_handler(): Parse exception (line " << e.getLine() << "): " << e.getError();
	throw std::runtime_error(ss.str());
    } catch(std::runtime_error &e) {
	throw e;
    } catch(...) {
	throw std::runtime_error("object_handler::object_handler(): Unknown exception");
    }

    // set the random number generator seed
    rnd.seed(boost::posix_time::microsec_clock::local_time().time_of_day().total_nanoseconds());
        
    DBG("constructor init complete");
 }

object_handler::~object_handler() {
    delete direct_rpc;
    delete query;
    delete dht;
}

void object_handler::rpc_provider_callback(buffer_wrapper page_key, interval_range_query::replica_policy_t &repl, 
				  buffer_wrapper buffer, bool &result,
				  const rpcreturn_t &error, const rpcvector_t &/*val*/) {
    if (error == rpcstatus::ok) 
	return;

    provider_adv adv = repl.try_next();
    INFO("could not fetch page: " << page_key << " from location: " << adv << ", error is " << error);
    if (adv.empty()) {
	ERROR("no more replicas for page: " << page_key << ", aborting");
	result = false;
	return;
    }
    rpcvector_t read_params;
    read_params.push_back(page_key);
    INFO("trying next replica for page " << page_key << ", location: " << adv);
    direct_rpc->dispatch(adv.get_host(), adv.get_service(), PROVIDER_READ, read_params,
			 boost::bind(&object_handler::rpc_provider_callback, this, page_key, boost::ref(repl), buffer, boost::ref(result), _1, _2),
			 rpcvector_t(1, buffer));
}

static void rpc_write_callback(boost::dynamic_bitset<> &res, unsigned int k, unsigned int rc, 
			       const rpcreturn_t &error, const rpcvector_t &) {
    res[k] = (error == rpcstatus::ok);
    if (!res[k])
	INFO("replica " << k % rc << " of page " << k / rc << " could not be written successfully, error is: " << error);
}

static void rpc_result_callback(bool &res, const rpcreturn_t &error, const rpcvector_t &) {
    if (error != rpcstatus::ok) {
	res = false;
	ERROR("could not perform RPC successfully, error is: " << error);
    }
}

template <class T> static void rpc_get_serialized(bool &res, T &output, const rpcreturn_t &error, const rpcvector_t &result) {
    if (error == rpcstatus::ok && result.size() == 1 && result[0].getValue(&output, true))
	return;
    res = false;
}

bool object_handler::get_root(boost::uint32_t version, metadata::root_t &root) {
    bool result = true;

    if (version == 0 || !version_cache.read(version, &root)) {
	rpcvector_t params;
	params.push_back(buffer_wrapper(id, true));
	params.push_back(buffer_wrapper(version, true));
	direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_GETROOT, params,
			     bind(&rpc_get_serialized<metadata::root_t>, boost::ref(result), boost::ref(root), _1, _2));
	direct_rpc->run();
	if (result)
	    version_cache.write(root.node.version, root);
    }

    return result;
}

bool object_handler::get_locations(page_locations_t &loc, boost::uint64_t offset, boost::uint64_t size, 
				   boost::uint32_t version) {
    metadata::root_t query_root(0, 0, 0, 0, 0);

    if (version == 0)
	query_root = latest_root;
    else
	if (!get_root(version, query_root))
	    return false;

    DBG("query root = " << query_root.node <<  ", version = " << version);

    if (query_root.node.version < version)
	throw std::runtime_error("object_handler::get_locations(): requested version higher than latest available version");
    if (query_root.page_size == 0)
	throw std::runtime_error("object_handler::get_locations(): read attempt on unallocated/uninitialized object");
    if (offset + size > query_root.node.size)
	throw std::runtime_error("object_handler::get_locations(): read attempt beyond maximal size");
    
    TIMER_START(read_locations);
    //std::vector<random_select> vadv(size / query_root.page_size);
    boost::uint64_t psize = query_root.page_size;
    boost::uint64_t new_offset = (offset / psize) * psize;
    boost::uint64_t nbr_vadv = (size + offset - new_offset) / psize + (((offset + size) % psize == 0) ? 0 : 1);
    boost::uint64_t new_size = nbr_vadv * psize;
    std::vector<random_select> vadv(nbr_vadv);

    metadata::query_t range(query_root.node.id, query_root.node.version, new_offset, new_size);
    //metadata::query_t range(query_root.node.id, query_root.node.version, offset, size);

    TIMER_START(meta_timer);
    bool result = query->readRecordLocations(vadv, range, query_root);
    TIMER_STOP(meta_timer, "GET_LOCATIONS " << range << ": Metadata read operation, success: " << result);
    if (!result)
	return false;

    provider_adv adv;
    for (boost::uint64_t i = 0; i < vadv.size(); i++)
	while (1) {
	    provider_adv adv = vadv[i].try_next();
	    if (adv.empty()) 
		break;
	    loc.push_back(page_location_t(adv.get_host(), adv.get_service(), 
					  i * query_root.page_size, query_root.page_size));
	}
    TIMER_STOP(read_locations, "GET_LOCATIONS " << range << ": Page location vector has been successfully constructed");

    return true;
}

bool object_handler::read(boost::uint64_t offset, boost::uint64_t size, char *buffer, boost::uint32_t version) {
    metadata::root_t query_root(0, 0, 0, 0, 0);

    if (version == 0)
	query_root = latest_root;
    else
	if (!get_root(version, query_root))
	    return false;

    DBG("query root = " << query_root.node <<  ", version = " << version);

    if (query_root.node.version < version)
	throw std::runtime_error("object_handler::read(): read attempt on a version higher than latest available version");
    if (query_root.page_size == 0)
	throw std::runtime_error("object_handler::read(): read attempt on unallocated/uninitialized object");
    if (offset + size > query_root.node.size)
	throw std::runtime_error("object_handler::read(): read attempt beyond maximal size");

    TIMER_START(read_timer);
    //std::vector<random_select> vadv(size / query_root.page_size);
    boost::uint64_t psize = query_root.page_size;
    boost::uint64_t new_offset = (offset / psize) * psize;
    boost::uint64_t nbr_vadv = (size + offset - new_offset) / psize + (((offset + size) % psize == 0) ? 0 : 1);
    boost::uint64_t new_size = nbr_vadv * psize;
    std::vector<random_select> vadv(nbr_vadv);

    metadata::query_t range(query_root.node.id, query_root.node.version, new_offset, new_size);
    //metadata::query_t range(query_root.node.id, query_root.node.version, offset, size);

    TIMER_START(meta_timer);
    bool result = query->readRecordLocations(vadv, range, query_root);
    TIMER_STOP(meta_timer, "READ " << range << ": Metadata read operation, success: " << result);
    if (!result)
	return false;

    rpcvector_t read_params;
    TIMER_START(data_timer);
    
    // size left to read from left/right page in an unaligned read
    uint64_t left_part = (query_root.page_size - (offset % query_root.page_size)) % query_root.page_size;
    uint64_t right_part = (offset + size) %  query_root.page_size;

    buffer_wrapper left_buffer, right_buffer;
    // left side is unaligned
    unsigned int l = 0;
    if(offset % psize != 0) {
	l++;
	metadata::query_t page_key(range.id, vadv[0].get_version(), vadv[0].get_index(), query_root.page_size);
	DBG("READ QUERY " << page_key);
	read_params.clear();
	read_params.push_back(buffer_wrapper(page_key, true));
	DBG("PAGE KEY IN SERIAL FORM " << read_params.back());
	provider_adv adv = vadv[0].try_next();
	if (adv.empty())
	    return false;
	left_buffer = buffer_wrapper(new char[psize], query_root.page_size);
	direct_rpc->dispatch(adv.get_host(), adv.get_service(), PROVIDER_READ, read_params,
			     boost::bind(&object_handler::rpc_provider_callback, this, read_params.back(), 
					 boost::ref(vadv[0]), left_buffer, boost::ref(result), _1, _2),
			     rpcvector_t(1, left_buffer));
    }
    // right side is unaligned
    unsigned int r = vadv.size();
    if((offset + size) % psize != 0 && r != 1) {
	r--;
	metadata::query_t page_key(range.id, vadv[r].get_version(), vadv[r].get_index(), query_root.page_size);
	DBG("READ QUERY " << page_key);
	read_params.clear();
	read_params.push_back(buffer_wrapper(page_key, true));
	DBG("PAGE KEY IN SERIAL FORM " << read_params.back());
	provider_adv adv = vadv[r].try_next();
	if (adv.empty())
	    return false;

	right_buffer = buffer_wrapper(new char[psize], query_root.page_size);
	direct_rpc->dispatch(adv.get_host(), adv.get_service(), PROVIDER_READ, read_params,
			     boost::bind(&object_handler::rpc_provider_callback, this, read_params.back(), 
					     boost::ref(vadv[r]), right_buffer, boost::ref(result), _1, _2),
			     rpcvector_t(1, right_buffer));
    }
    // every intermediate pages
    for (unsigned int i = l; result && i < r; i++) {
	metadata::query_t page_key(range.id, vadv[i].get_version(), vadv[i].get_index(), query_root.page_size);
	DBG("READ QUERY " << page_key);
	read_params.clear();
	read_params.push_back(buffer_wrapper(page_key, true));
	DBG("PAGE KEY IN SERIAL FORM " << read_params.back());
	provider_adv adv = vadv[i].try_next();
	if (adv.empty())
	    return false;
	
	buffer_wrapper wr_buffer(buffer + left_part + (i - l) * query_root.page_size, query_root.page_size, true);
	direct_rpc->dispatch(adv.get_host(), adv.get_service(), PROVIDER_READ, read_params,
			     boost::bind(&object_handler::rpc_provider_callback, this, read_params.back(), 
					 boost::ref(vadv[i]), wr_buffer, boost::ref(result), _1, _2),
			     rpcvector_t(1, wr_buffer));
    }
    direct_rpc->run();
    TIMER_STOP(data_timer, "READ " << range << ": Data read operation, success: " << result);
    
    // copy left buffer if needed
    if(offset % psize != 0) {
	vadv.size() == 1 ? memcpy(buffer, &((left_buffer.get())[psize - left_part]), size) :
	    memcpy(buffer, &((left_buffer.get())[psize - left_part]), left_part);
    }
    // copy right buffer if needed
    if((offset + size) % psize != 0 && vadv.size() > 1)
	memcpy(&(buffer[size - right_part]), right_buffer.get(), right_part);
    
    TIMER_STOP(read_timer, "READ " << range << ": has completed");
    return result;
}

bool object_handler::append(boost::uint64_t size, char *buffer) {
    return exec_write(0, size, buffer, true);
}

bool object_handler::write(boost::uint64_t offset, boost::uint64_t size, char *buffer) {
    return exec_write(offset, size, buffer, false);
}

bool object_handler::exec_write(boost::uint64_t offset, boost::uint64_t size, char *buffer, bool append) {
    if (latest_root.page_size == 0)
	throw std::runtime_error("object_handler::write(): write attempt on unallocated/uninitialized object");
    ASSERT(offset % latest_root.page_size == 0 && size % latest_root.page_size == 0);

    TIMER_START(write_timer);
    bool result = true;

    metadata::query_t range(id, rnd(), offset, size);
    boost::uint64_t page_size = latest_root.page_size;
    unsigned int replica_count = latest_root.replica_count;

    std::vector<provider_adv> adv;
    interval_range_query::node_deque_t node_deque;
    rpcvector_t params;

    // try to get a list of providers
    TIMER_START(publisher_timer);
    params.clear();
    params.push_back(buffer_wrapper((size / page_size) * replica_count, true));
    direct_rpc->dispatch(publisher_host, publisher_service, PUBLISHER_GET, params,
			 boost::bind(&rpc_get_serialized<std::vector<provider_adv> >, boost::ref(result), boost::ref(adv), _1, _2));
    direct_rpc->run();
    TIMER_STOP(publisher_timer, "WRITE " << range << ": PUBLISHER_GET, result: " << result);
    if (!result || adv.size() < (size / page_size) * replica_count)
	return false;

    // write the set of pages to the page providers
    boost::dynamic_bitset<> page_results(adv.size());
    TIMER_START(providers_timer);
    for (boost::uint64_t i = 0, j = 0; i * page_size < size; i++, j += replica_count) {
	// prepare the page
	rpcvector_t write_params;
	metadata::query_t page_key(id, range.version, i, page_size);
	DBG("WRITE QUERY " << page_key);
	write_params.push_back(buffer_wrapper(page_key, true));
	DBG("PAGE KEY IN SERIAL FORM " << write_params.back());
	write_params.push_back(buffer_wrapper(buffer + i * page_size, page_size, true));
	// write the replicas
	for (unsigned int k = j; k < j + replica_count; k++) {
	    direct_rpc->dispatch(adv[k].get_host(), adv[k].get_service(), PROVIDER_WRITE, 
				 write_params, boost::bind(rpc_write_callback, boost::ref(page_results), k, replica_count, _1, _2));
	    // set the version & page index for leaf nodes
	    adv[k].set_free(range.version);
	    adv[k].set_update_rate(i);
	}
    }
    direct_rpc->run();
    
    // make sure each page has at least one successfully written replica
    for (unsigned int i = 0; i < adv.size() && result; i++) {
	unsigned int k;
	for (k = i; k < i + replica_count; k++)
	    if (page_results[k])
		break;
	if (k == i + replica_count) {
	    ERROR("WRITE " << range << ": none of the replicas of page " << i / replica_count 
		  << " could be written successfully, aborted");
	    result = false;
	    break;
	}	    
    }
	
    TIMER_STOP(providers_timer, "WRITE " << range << ": Data written to providers, result: " << result);
    if (!result)
	return false;
    
    // get a ticket from the version manager
    params.clear();
    params.push_back(buffer_wrapper(range, true));
    params.push_back(buffer_wrapper(append, true));

    vmgr_reply mgr_reply;
    TIMER_START(ticket_timer);
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_GETTICKET, params,
			 bind(&rpc_get_serialized<vmgr_reply>, boost::ref(result), boost::ref(mgr_reply), _1, _2));
    direct_rpc->run();
    TIMER_STOP(ticket_timer, "WRITE " << range << ": VMGR_GETTICKET, result: " << result);
    if (!result)
	return false;

    // construct the set of leaves to be written to the metadata
    range.offset = offset = mgr_reply.append_offset;
    range.version = mgr_reply.ticket;
    for (boost::uint64_t i = offset; i < offset + size; i += page_size)
	node_deque.push_back(metadata::query_t(id, mgr_reply.ticket, i, page_size));

    TIMER_START(metadata_timer);
    result = query->writeRecordLocations(mgr_reply, node_deque, adv);
    TIMER_STOP(metadata_timer, "WRITE " << range << ": writeRecordLocations(), result: " << result);
    if (!result)
	return false;

    // publish the latest written version
    TIMER_START(publish_timer);
    params.clear();
    params.push_back(buffer_wrapper(range, true));
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_PUBLISH, params,
			 boost::bind(rpc_result_callback, boost::ref(result), _1, _2));
    direct_rpc->run();
    TIMER_STOP(publish_timer, "WRITE " << range << ": VMGR_PUBLISH, result: " << result);
    TIMER_STOP(write_timer, "WRITE " << range << ": has completed, result: " << result);
    if (result) {
	latest_root.node.version = mgr_reply.ticket;
	return true;
    } else
	return false;
}

bool object_handler::create(boost::uint64_t page_size, boost::uint32_t replica_count) {
    bool result = true;

    rpcvector_t params;
    params.push_back(buffer_wrapper(page_size, true));
    params.push_back(buffer_wrapper(replica_count, true));
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_CREATE, params,
			 boost::bind(rpc_get_serialized<metadata::root_t>, boost::ref(result), boost::ref(latest_root), _1, _2));
    direct_rpc->run();
    id = latest_root.node.id;
    DBG("create result = " << result << ", id = " << id);
    return result;
}

bool object_handler::get_latest(boost::uint32_t id_) {
    if (id_ != 0)
	id = id_;

    bool result = get_root(0, latest_root);
    DBG("latest version request: " << latest_root.node);
    return result;
}

boost::int32_t object_handler::get_objcount() const {
    bool result = true;
    boost::int32_t obj_no;

    rpcvector_t params;    
    direct_rpc->dispatch(vmgr_host, vmgr_service, VMGR_GETOBJNO, rpcvector_t(), 
			 boost::bind(rpc_get_serialized<boost::int32_t>, boost::ref(result), boost::ref(obj_no), _1, _2));
    direct_rpc->run();
    DBG("the total number of blobs is: " << obj_no);
    if (result)
	return obj_no;
    else
	return -1;
}
