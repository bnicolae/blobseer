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
    typedef rpc_client<config::socket_namespace> rpc_client_t;

    static const unsigned int UPDATE_TIMEOUT = 5;

    std::string phost, pservice, service;
    boost::uint64_t free_space;
    rpc_client_t publisher;
    boost::asio::deadline_timer timeout_timer;

    void provider_callback(const rpcreturn_t &error, const rpcvector_t &answer);
    void timeout_callback(const boost::system::error_code& error);

public:
    pmgr_listener(boost::asio::io_service &io_service, 
		  const std::string &ph, const std::string &ps,
		  const boost::uint64_t fs,
		  const std::string &service);
    void update_event(const boost::int32_t name, const monitored_params_t &params);
    ~pmgr_listener();
};

#endif
