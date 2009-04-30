#ifndef __PAGE_MANAGER
#define __PAGE_MANAGER

#include <vector>
#include <boost/tuple/tuple.hpp>
#include <boost/function.hpp>

#include "provider/provider.hpp"
#include "rpc/rpc_meta.hpp"
#include "common/cache_mt.hpp"
#include "common/bdb_bw_map.hpp"
#include "common/config.hpp"

typedef boost::tuple<boost::int32_t, buffer_wrapper, boost::uint64_t, std::string> monitored_params_t;

template <class Persistency> class page_manager {
private:
    typedef Persistency page_cache_t;
    typedef boost::function<void (const boost::int32_t, const monitored_params_t &) > update_hook_t;
    typedef std::vector<update_hook_t> update_hooks_t;

public:
    page_manager(const std::string &db_name, boost::uint64_t cs, boost::uint64_t ts, unsigned int to);
    ~page_manager();

    rpcreturn_t free_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t write_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t read_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);

    void add_listener(update_hook_t hook);

private:    
    page_cache_t *page_cache;
    update_hooks_t update_hooks;

    void exec_hooks(const boost::int32_t rpc_name, buffer_wrapper page_id, boost::uint64_t page_size, const std::string &sender);
};

template <class Persistency> page_manager<Persistency>::page_manager(const std::string &db_name, boost::uint64_t cs, 
									   boost::uint64_t ms, unsigned int to) : 
    page_cache(new page_cache_t(db_name, cs, ms, to)) { }

template <class Persistency> page_manager<Persistency>::~page_manager() {
    delete page_cache;
}

template <class Persistency> rpcreturn_t page_manager<Persistency>::write_page(const rpcvector_t &params, 
									       rpcvector_t & /*result*/, const std::string &sender) {
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

template <class Persistency> rpcreturn_t page_manager<Persistency>::read_page(const rpcvector_t &params, rpcvector_t &result, 
									      const std::string &sender) {
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

template <class Persistency> void page_manager<Persistency>::exec_hooks(const boost::int32_t rpc_name, buffer_wrapper page_id, const boost::uint64_t ps, const std::string &sender) {
    for (update_hooks_t::iterator i = update_hooks.begin(); i != update_hooks.end(); ++i)
	(*i)(rpc_name, monitored_params_t(page_cache->get_free(), page_id, ps, sender));
}

template <class Persistency> void page_manager<Persistency>::add_listener(update_hook_t hook) {
    update_hooks.push_back(hook);
}

#endif
