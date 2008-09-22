#include "page_manager.hpp"
#include "common/debug.hpp"

page_manager::page_manager(unsigned int m) : 
    max_alloc(m), page_cache(new page_cache_t(m)) { }

page_manager::~page_manager() {
    delete page_cache;
}

rpcreturn_t page_manager::write_page(const rpcvector_t &params, rpcvector_t & /*result*/) {
    if (params.size() % 2 != 0 || params.size() < 2) {
	ERROR("RPC error: wrong argument number, required even");
	return rpcstatus::ok;
    }
    for (unsigned int i = 0; i < params.size(); i += 2)
	if (!page_cache->write(params[i], params[i + 1])) {
	    ERROR("memory is full");
	    return rpcstatus::eres;
	}
    INFO("RPC successful");
    return rpcstatus::ok;
}

rpcreturn_t page_manager::read_page(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() < 1) {
	ERROR("RPC error: wrong argument number, required at least one");
	return rpcstatus::earg;
    }
    // code to be executed
    for (unsigned int i = 0; i < params.size(); i++) {
	buffer_wrapper data;
	if (!page_cache->read(params[i], &data)) {
	    ERROR("page was not found: " << params[i]);
	    return rpcstatus::eobj;
	}	
	result.push_back(data);
    }
    return rpcstatus::ok;
}
