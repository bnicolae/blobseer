#ifndef __PAGE_MANAGER
#define __PAGE_MANAGER

#include <vector>
#include <boost/tuple/tuple.hpp>
#include <boost/function.hpp>

#include "provider/provider.hpp"
#include "rpc/rpc_meta.hpp"
#include "common/cache_mt.hpp"
#include "common/config.hpp"

class page_manager {
public:
    typedef boost::tuple<boost::uint64_t, buffer_wrapper> monitored_params_t;

private:
    typedef cache_mt<buffer_wrapper, buffer_wrapper, config::lock_t, buffer_wrapper_hash, cache_mt_none<buffer_wrapper> > page_cache_t;
    typedef boost::function<void (const boost::int32_t, const monitored_params_t &) > update_hook_t;
    typedef std::vector<update_hook_t> update_hooks_t;

    page_cache_t *page_cache;
    update_hooks_t update_hooks;

    void exec_hooks(const boost::int32_t rpc_name, buffer_wrapper page_id);

public:
    page_manager(boost::uint64_t max);
    ~page_manager();

    rpcreturn_t free_page(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t write_page(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t read_page(const rpcvector_t &params, rpcvector_t &result);

    void add_listener(update_hook_t hook);
};

#endif
