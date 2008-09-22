#include <sstream>

#include "object_handler.hpp"
#include "publisher/publisher.hpp"
#include "provider/provider.hpp"
#include "lockmgr/lockmgr.hpp"
#include "libconfig.h++"
#include "common/debug.hpp"

using namespace std;

object_handler::object_handler(const std::string &config_file) : latest_root(0, 0, 0, 0) {
    libconfig::Config cfg;
    
    try {
        cfg.readFile(config_file.c_str());
	// get dht port
	std::string service;
	if (!cfg.lookupValue("dht.port", service))
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
	int max_threads;
	if (!cfg.lookupValue("dht.threads", max_threads) || 
	    !cfg.lookupValue("publisher.host", publisher_host) ||
	    !cfg.lookupValue("publisher.service", publisher_service) ||
	    !cfg.lookupValue("lockmgr.host", lockmgr_host) ||
	    !cfg.lookupValue("lockmgr.service", lockmgr_service))
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
    DBG("constructor init complete");
}

object_handler::~object_handler() {
    delete direct_rpc;
    delete query;
    delete dht;
}

static void rpc_provider_callback(bool &res, const rpcreturn_t &error, const rpcvector_t &/*val*/) {
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

bool object_handler::read(uint64_t offset, uint64_t size, char *buffer) {
    if (latest_root.page_size == 0) 
	throw std::runtime_error("object_handler::read(): read attempt on unallocated/uninitialized object");

    TIMER_START(read_timer);
    std::vector<provider_adv> adv(size / latest_root.page_size);
    
    metadata::query_t range(latest_root.node.id, latest_root.node.version, offset, size);
    TIMER_START(meta_timer);
    bool result = query->readRecordLocations(adv, range, latest_root);
    TIMER_STOP(meta_timer, "READ " << range << ": Metadata read operation, success: " << result);
    if (!result) 
	return false;    

    TIMER_START(data_timer);
    for (unsigned int i = 0; result && i < adv.size(); i++) 
	if (!adv[i].empty()) {
	    metadata::query_t page_key(range.id, adv[i].get_free(), offset + i * latest_root.page_size, latest_root.page_size);
	    DBG("READ PAGE KEY " << adv[i]);
	    rpcvector_t read_params;
	    read_params.push_back(buffer_wrapper(page_key, true));
	    DBG("PAGE KEY IN SERIAL FORM " << read_params.back());
	    direct_rpc->dispatch(adv[i].get_host(), adv[i].get_service(), PROVIDER_READ, read_params,
				 boost::bind(rpc_provider_callback, boost::ref(result), _1, _2),
				 rpcvector_t(1, buffer_wrapper(buffer + i * latest_root.page_size, latest_root.page_size, true)));

	} else
	    return false;
	    
    direct_rpc->run();
    TIMER_STOP(data_timer, "READ " << range << ": Data read operation, success: " << result);
    TIMER_STOP(read_timer, "READ " << range << ": has completed");
    return result;
}

bool object_handler::write(uint64_t offset, uint64_t size, char *buffer) {
    if (latest_root.page_size == 0)
	throw std::runtime_error("object_handler::write(): write attempt on unallocated/uninitialized object");

    TIMER_START(write_timer);
    bool result = true;

    metadata::query_t range(id, 0, offset, size);
    //uint64_t max_size = latest_root.node.size;
    uint64_t page_size = latest_root.page_size;

    TIMER_START(lockpv_timer);
    rpcvector_t params;
    params.push_back(buffer_wrapper(range, true));
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, LOCKMGR_GETRANGEVER, params,
			 boost::bind(&rpc_get_serialized<metadata::query_t>, boost::ref(result), boost::ref(range), _1, _2));
    direct_rpc->run();
    TIMER_STOP(lockpv_timer, "WRITE " << range << ": LOCKMGR_GETRANGEVER, operation success: " << result);
    if (!result)
	return false;

    std::vector<provider_adv> adv;
    interval_range_query::node_deque_t node_deque;
    // try to get a list of providers
    TIMER_START(publisher_timer);
    params.clear();
    params.push_back(buffer_wrapper(size / page_size, true));
    direct_rpc->dispatch(publisher_host, publisher_service, PUBLISHER_GET, params,
			 boost::bind(&rpc_get_serialized<std::vector<provider_adv> >, boost::ref(result), boost::ref(adv), _1, _2));
    direct_rpc->run();
    TIMER_STOP(publisher_timer, "WRITE " << range << ": PUBLISHER_GET, operation success: " << result);
    if (!result || adv.size() < size / page_size)
	return false;

    TIMER_START(providers_timer);
    for (uint64_t i = offset, j = 0; i < offset + size; i += page_size, j++) {
	// write the page
	rpcvector_t write_params;
	write_params.push_back(buffer_wrapper(metadata::query_t(id, range.version, i, page_size), true));
	write_params.push_back(buffer_wrapper(buffer + i - offset, page_size, true));
	direct_rpc->dispatch(adv[j].get_host(), adv[j].get_service(), PROVIDER_WRITE, 
			     write_params, boost::bind(rpc_provider_callback, boost::ref(result), _1, _2));
	if (!result)
	    break;
	// set the version for leaf nodes
	adv[j].set_free(range.version);
    }
    direct_rpc->run();
    TIMER_STOP(providers_timer, "WRITE " << range << ": Data written to providers, operation success: " << result);
    if (!result)
	return false;
    
    // get a ticket from the lock manager
    params.clear();    
    params.push_back(buffer_wrapper(range, true));
    if (page_size <= size) {
	params.push_back(buffer_wrapper(metadata::query_t(id, 0, offset, page_size), true));
	params.push_back(buffer_wrapper(metadata::query_t(id, 0, offset + size - page_size, page_size), true));
    }
    lockmgr_reply mgr_reply;
    TIMER_START(ticket_timer);
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, LOCKMGR_GETTICKET, params,
			 bind(&rpc_get_serialized<lockmgr_reply>, boost::ref(result), boost::ref(mgr_reply), _1, _2));
    direct_rpc->run();
    TIMER_STOP(ticket_timer, "WRITE " << range << ": LOCKMGR_GETTICKET, operation success: " << result);
    if (!result)
	return false;

    // construct a list of pages to be written to the metadata
    for (uint64_t i = offset; i < offset + size; i += page_size)
	node_deque.push_back(metadata::query_t(id, mgr_reply.ticket, i, page_size));

    TIMER_START(metadata_timer);
    result = query->writeRecordLocations(mgr_reply, node_deque, adv);
    TIMER_STOP(metadata_timer, "WRITE " << range << ": writeRecordLocations(), operation success: " << result);
    if (!result)
	return false;

    // publish the latest written version
    TIMER_START(publish_timer);
    range.version = mgr_reply.ticket;
    params.clear();
    params.push_back(buffer_wrapper(range, true));
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, LOCKMGR_PUBLISH, params,
			 boost::bind(rpc_provider_callback, boost::ref(result), _1, _2));
    direct_rpc->run();
    TIMER_STOP(publish_timer, "WRITE " << range << ": LOCKMGR_PUBLISH, operation success: " << result);
    TIMER_STOP(write_timer, "WRITE " << range << ": has completed" << result);
    if (result) {
	latest_root.node.version = mgr_reply.ticket;
	return true;
    } else
	return false;
}

bool object_handler::create(uint64_t page_size) {
    bool result = true;

    rpcvector_t params;
    params.push_back(buffer_wrapper(page_size, true));
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, LOCKMGR_CREATE, params,
			 boost::bind(rpc_get_serialized<metadata::root_t>, boost::ref(result), boost::ref(latest_root), _1, _2));
    direct_rpc->run();
    id = latest_root.node.id;
    INFO("create result = " << result << ", id = " << id);
    return result;
}

uint64_t object_handler::get_latest(uint32_t id_) {
    bool result = true;

    rpcvector_t params;
    if (id_ != 0)
	id = id_;
    params.push_back(buffer_wrapper(id, true));
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, LOCKMGR_LASTVER, params, 
			 boost::bind(rpc_get_serialized<metadata::root_t>, boost::ref(result), boost::ref(latest_root), _1, _2));
    direct_rpc->run();
    INFO("latest version request: " << latest_root.node);
    if (result) 
	return latest_root.node.size;
    else
	return 0;
}

int32_t object_handler::get_objcount() {
    bool result = true;
    int32_t obj_no;

    rpcvector_t params;    
    direct_rpc->dispatch(lockmgr_host, lockmgr_service, LOCKMGR_GETOBJNO, rpcvector_t(), 
			 boost::bind(rpc_get_serialized<int32_t>, boost::ref(result), boost::ref(obj_no), _1, _2));
    direct_rpc->run();
    INFO("latest version request: " << latest_root.node);
    if (result)
	return obj_no;
    else
	return -1;
}
