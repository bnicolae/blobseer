#ifndef __PAGE_MANAGER
#define __PAGE_MANAGER

#include <vector>
#include <boost/tuple/tuple.hpp>
#include <boost/function.hpp>

#include "provider/provider.hpp"
#include "rpc/rpc_meta.hpp"
#include "common/cache_mt.hpp"
#include "common/config.hpp"

typedef boost::tuple<boost::uint64_t, buffer_wrapper, boost::uint64_t, std::string> monitored_params_t;

template <class Persistency> class page_manager {
private:
    typedef Persistency page_cache_t;
    typedef boost::function<void (const boost::int32_t, const monitored_params_t &) > update_hook_t;
    typedef std::vector<update_hook_t> update_hooks_t;

public:
    page_manager(page_cache_t *pc, bool compression = false);
    ~page_manager();

    rpcreturn_t free_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t write_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t read_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t read_partial_page(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);

    void add_listener(update_hook_t hook);

private:    
    page_cache_t *page_cache;
    update_hooks_t update_hooks;
    bool compression;

    void exec_hooks(const boost::int32_t rpc_name, buffer_wrapper page_id, boost::uint64_t page_size, const std::string &sender);
};

template <class Persistency> page_manager<Persistency>::page_manager(page_cache_t *pc, bool c) 
    : page_cache(pc), compression(c) { }

template <class Persistency> page_manager<Persistency>::~page_manager() { }

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
    unsigned int ok = 0;
    // code to be executed
    for (unsigned int i = 0; i < params.size(); i++) {
	buffer_wrapper data;
	if (!page_cache->read(params[i], &data)) {
	    INFO("page could not be read: " << params[i]);
	    result.push_back(buffer_wrapper());
	} else {
	    exec_hooks(PROVIDER_READ, params[i], data.size(), sender);	
	    result.push_back(data);
	    ok++;
	}
    }
    if (ok == params.size())
	return rpcstatus::ok;
    else
	return rpcstatus::eobj;
}

template <class Persistency> rpcreturn_t page_manager<Persistency>::read_partial_page(const rpcvector_t &params, rpcvector_t &result, 
										      const std::string &sender) {
    boost::uint64_t offset, size;
    buffer_wrapper data, out;

    if (params.size() != 3) {
	ERROR("RPC error: wrong argument number, required are three: page key, offset, size");
	return rpcstatus::earg;
    }
    // code to be executed
    if (!params[1].getValue(&offset, true) || !params[2].getValue(&size, true)) {
	ERROR("RPC error: could not deserialize offset and size for partial read, aborted");
	return rpcstatus::earg;
    }
    if (!page_cache->read(params[0], &out)) {
	INFO("page could not be read: " << params[0]);
	return rpcstatus::eobj;
    }
    if (compression)
	data.decompress(out.get(), out.size());
    else
	data = out;
    if (data.size() < offset + size) {
	INFO("offset " << offset << " and size " << size << " do not fall within the requested page " << params[0]);
	return rpcstatus::eobj;
    }
    result.push_back(buffer_wrapper(data.get() + offset, size, true));
    exec_hooks(PROVIDER_READ, params[0], size, sender);

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
