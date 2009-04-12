#include <sstream>
#include <libconfig.h++>

#include "pmanager/publisher.hpp"
#include "provider/provider.hpp"
#include "vmanager/main.hpp"
#include "common/debug.hpp"

#include "object_handler.hpp"
#include "replica_policy.hpp"

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
	    !cfg.lookupValue("vmanager.host", lockmgr_host) ||
	    !cfg.lookupValue("vmanager.service", lockmgr_service))
	    throw std::runtime_error("object_handler::object_handler(): object_handler parameters are missing/invalid");
	// complete object creation
	query = new interval_range_query(dht);
	direct_rpc = new rpc_client_t(io_service);
    } catch(libconfig::FileIOException) {
	throw std::runtime_error("object_handler::object_handler(): I/O error trying to parse config file");
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
    INFO("could not fetch page: " << page_key << ", error is " << error);
    provider_adv adv = repl.try_next();
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

static void rpc_write_callback(bool &res, const rpcreturn_t &error, const rpcvector_t &/*val*/) {
    if (error != rpcstatus::ok) {
	ERROR("error is: " << error);
 	res = false;
    }
}

template <class T> static void rpc_get_serialized(bool &res, T &output, const rpcreturn_t &error, const rpcvector_t &result) {
    if (error == rpcstatus::ok && result.size() == 1 && result[0].getValue(&output, true))
	return;
    res = false;
}

bool object_handler::read(boost::uint64_t offset, boost::uint64_t size, char *buffer) {
    if (latest_root.page_size == 0)
	throw std::runtime_error("object_handler::read(): read attempt on unallocated/uninitialized object");
    if (offset + size > latest_root.node.size)
	throw std::runtime_error("object_handler::read(): read attempt beyond maximal size");
    ASSERT(offset % latest_root.page_size == 0 && size % latest_root.page_size == 0);

    TIMER_START(read_timer);
    std::vector<random_select> vadv(size / latest_root.page_size);

    metadata::query_t range(latest_root.node.id, latest_root.node.version, offset, size);
    TIMER_START(meta_timer);
    bool result = query->readRecordLocations(vadv, range, latest_root);
    TIMER_STOP(meta_timer, "READ " << range << ": Metadata read operation, success: " << result);
    if (!result)
	return false;

    TIMER_START(data_timer);
    for (unsigned int i = 0; result && i < vadv.size(); i++) {
	metadata::query_t page_key(range.id, vadv[i].get_version(), i * latest_root.page_size, latest_root.page_size);
	DBG("READ QUERY " << page_key);
	rpcvector_t read_params;
	read_params.push_back(buffer_wrapper(page_key, true));
	DBG("PAGE KEY IN SERIAL FORM " << read_params.back());
	provider_adv adv = vadv[i].try_next();
	if (adv.empty())
	    return false;
	buffer_wrapper wr_buffer(buffer + i * latest_root.page_size, latest_root.page_size, true);
	direct_rpc->dispatch(adv.get_host(), adv.get_service(), PROVIDER_READ, read_params,
			     boost::bind(&object_handler::rpc_provider_callback, this, read_params.back(), 
					 boost::ref(vadv[i]), wr_buffer, boost::ref(result), _1, _2),
			     rpcvector_t(1, wr_buffer));
    }
    direct_rpc->run();
    TIMER_STOP(data_timer, "READ " << range << ": Data read operation, success: " << result);
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

    TIMER_START(providers_timer);
    for (boost::uint64_t i = 0, j = 0; i < size && result; i += page_size, j += replica_count) {
	// prepare the page
	rpcvector_t write_params;
	write_params.push_back(buffer_wrapper(metadata::query_t(id, range.version, i, page_size), true));
	write_params.push_back(buffer_wrapper(buffer + i, page_size, true));
	// write the replicas
	for (unsigned int k = j; k < j + replica_count && result; k++) {
	    direct_rpc->dispatch(adv[k].get_host(), adv[k].get_service(), PROVIDER_WRITE, 
				 write_params, boost::bind(rpc_write_callback, boost::ref(result), _1, _2));
	    // set the version for leaf nodes
	    adv[k].set_free(range.version);
	}
    }
    direct_rpc->run();
    TIMER_STOP(providers_timer, "WRITE " << range << ": Data written to providers, result: " << result);
    if (!result)
	return false;
    
    // get a ticket from the version manager
    params.clear();
    params.push_back(buffer_wrapper(range, true));
    params.push_back(buffer_wrapper(append, true));

    vmgr_reply mgr_reply;
    TIMER_START(ticket_timer);
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, VMGR_GETTICKET, params,
			 bind(&rpc_get_serialized<vmgr_reply>, boost::ref(result), boost::ref(mgr_reply), _1, _2));
    direct_rpc->run();
    TIMER_STOP(ticket_timer, "WRITE " << range << ": VMGR_GETTICKET, result: " << result);
    if (!result)
	return false;

    // construct a list of pages to be written to the metadata
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
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, VMGR_PUBLISH, params,
			 boost::bind(rpc_write_callback, boost::ref(result), _1, _2));
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
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, VMGR_CREATE, params,
			 boost::bind(rpc_get_serialized<metadata::root_t>, boost::ref(result), boost::ref(latest_root), _1, _2));
    direct_rpc->run();
    id = latest_root.node.id;
    INFO("create result = " << result << ", id = " << id);
    return result;
}

bool object_handler::get_latest(boost::uint32_t id_, boost::uint64_t *size) {
    bool result = true;

    rpcvector_t params;
    if (id_ != 0)
	id = id_;
    params.push_back(buffer_wrapper(id, true));
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, VMGR_LASTVER, params, 
			 boost::bind(rpc_get_serialized<metadata::root_t>, boost::ref(result), boost::ref(latest_root), _1, _2));
    direct_rpc->run();
    INFO("latest version request: " << latest_root.node);
    if (result) {
	if (size)
	    *size = latest_root.current_size;
	return true;
    } else
	return false;
}

bool object_handler::set_version(unsigned int ver) {
    if (latest_root.node.version > ver) {
	latest_root.node.version = ver;
	return true;
    } else
	return false;
}

int32_t object_handler::get_objcount() const {
    bool result = true;
    int32_t obj_no;

    rpcvector_t params;    
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, VMGR_GETOBJNO, rpcvector_t(), 
			 boost::bind(rpc_get_serialized<int32_t>, boost::ref(result), boost::ref(obj_no), _1, _2));
    direct_rpc->run();
    INFO("latest version request: " << latest_root.node);
    if (result)
	return obj_no;
    else
	return -1;
}
