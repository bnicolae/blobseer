#ifndef __PMGR_LISTENER
#define __PMGR_LISTENER

#include <vector>

#include "common/config.hpp"
#include "rpc/rpc_client.hpp"
#include "provider/provider_adv.hpp"
#include "pmanager/publisher.hpp"
#include "provider/provider.hpp"
#include "provider/page_manager.hpp"

class pmgr_listener {
    typedef rpc_client<config::socket_namespace, config::lock_t> rpc_client_t;

    static const unsigned int RETRY_TIMEOUT = 10;

    std::string publisher_host, publisher_service;
    provider_adv adv;
    unsigned int update_rate, retry_count;
    rpc_client_t *publisher;
    boost::asio::deadline_timer *timeout_timer;

    void update(unsigned int retry_count);
    void provider_callback(unsigned int retry_count, const rpcreturn_t &error, const rpcvector_t &answer);
    void timeout_callback(unsigned int retry_count, const boost::system::error_code& error);

public:
    void update_event(const boost::int32_t name, const page_manager::monitored_params_t &params);
    pmgr_listener(boost::asio::io_service &io_service,
		  const provider_adv &adv, 
		  const std::string &phost, 
		  const std::string &pservice,
		  const unsigned int retry_count);
    ~pmgr_listener();
};

#endif
