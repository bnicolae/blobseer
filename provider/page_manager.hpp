#ifndef __PAGE_MANAGER
#define __PAGE_MANAGER

#include <vector>

#include "common/config.hpp"
#include "rpc/rpc_client.hpp"
#include "provider/provider_adv.hpp"
#include "provider/provider.hpp"
#include "publisher/publisher.hpp"

class page_manager {
    typedef cache_mt<buffer_wrapper, buffer_wrapper, null_lock, buffer_wrapper_hash, cache_mt_none<buffer_wrapper> > page_cache_t;
    typedef rpc_client<config::socket_namespace, config::lock_t> rpc_client_t;

    static const unsigned int RETRY_TIMEOUT = 10;

    std::string publisher_host, publisher_service;
    provider_adv adv;
    unsigned int update_rate;
    page_cache_t *page_cache;
    rpc_client_t *publisher;
    boost::asio::deadline_timer *timeout_timer;

    void update(unsigned int retry_count = 3);
    void provider_callback(unsigned int retry_count, const rpcreturn_t &error, const rpcvector_t &answer);
    void timeout_callback(unsigned int retry_count, const boost::system::error_code& error);
public:
    page_manager(boost::asio::io_service &io_service,
		 const provider_adv &adv, 
		 const std::string &phost, 
		 const std::string &pservice);
    ~page_manager();
    rpcreturn_t free_page(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t write_page(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t read_page(const rpcvector_t &params, rpcvector_t &result);
};

#endif
