#include "page_manager.hpp"

#include "common/debug.hpp"

page_manager::page_manager(const std::string &db_name, boost::uint64_t cs, boost::uint64_t ms, unsigned int to) : 
    page_cache(new page_cache_t(db_name, cs, ms, to)) { }

page_manager::~page_manager() {
    delete page_cache;
}

rpcreturn_t page_manager::write_page(const rpcvector_t &params, rpcvector_t & /*result*/, const std::string &sender) {
    if (params.size() % 2 != 0 || params.size() < 2) {
	ERROR("RPC error: wrong argument number, required even");
	return rpcstatus::ok;
    }
    for (unsigned int i = 0; i < params.size(); i += 2)
	if (!page_cache->write(params[i], params[i + 1])) {
	    ERROR("could not write page");
	    return rpcstatus::eres;
	} else {
	    exec_hooks(PROVIDER_WRITE, params[i], params[i + 1].size(), sender);
	    DBG("page written: " << params[i]);
	}
    return rpcstatus::ok;
}

rpcreturn_t page_manager::read_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender) {
    if (params.size() < 1) {
	ERROR("RPC error: wrong argument number, required at least one");
	return rpcstatus::earg;
    }
    // code to be executed
    for (unsigned int i = 0; i < params.size(); i++) {
	buffer_wrapper data;
	if (!page_cache->read(params[i], &data)) {
	    INFO("page could not be read: " << params[i]);
	    return rpcstatus::eobj;
	}	
	exec_hooks(PROVIDER_READ, params[i], data.size(), sender);
	result.push_back(data);
    }
    return rpcstatus::ok;
}

void page_manager::exec_hooks(const boost::int32_t rpc_name, buffer_wrapper page_id, const boost::uint64_t ps, const std::string &sender) {
    for (update_hooks_t::iterator i = update_hooks.begin(); i != update_hooks.end(); ++i)
	(*i)(rpc_name, monitored_params_t(page_cache->get_free(), page_id, ps, sender));
}

void page_manager::add_listener(update_hook_t hook) {
    update_hooks.push_back(hook);
}
