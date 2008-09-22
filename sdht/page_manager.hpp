#ifndef __PAGE_MANAGER
#define __PAGE_MANAGER

#include <vector>
#include <boost/system/error_code.hpp>

#include "common/config.hpp"
#include "rpc/rpc_client.hpp"

class page_manager {
    typedef cache_mt<buffer_wrapper, buffer_wrapper, config::lock_t, buffer_wrapper_hash, cache_mt_none<buffer_wrapper> > page_cache_t;

    unsigned int max_alloc;
    page_cache_t *page_cache;

public:
    page_manager(unsigned int max_alloc);
    ~page_manager();
    rpcreturn_t write_page(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t read_page(const rpcvector_t &params, rpcvector_t &result);
};

#endif
