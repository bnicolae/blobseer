#include "pmgr_listener.hpp"
#include "common/debug.hpp"

pmgr_listener::pmgr_listener(boost::asio::io_service &io_service,
		     const provider_adv &a, 
		     const std::string &phost, 
		     const std::string &pservice,
		     const unsigned int rc) : 
    publisher_host(phost), publisher_service(pservice), adv(a), retry_count(rc) {

    publisher = new rpc_client_t(io_service);
    timeout_timer = new boost::asio::deadline_timer(io_service);
    update_rate = adv.get_update_rate();
    update(~(unsigned int)0);
}

pmgr_listener::~pmgr_listener() {    
    delete publisher;
    delete timeout_timer;
}

void pmgr_listener::update_event(const boost::int32_t name, const page_manager::monitored_params_t &params) {
    switch (name) {
    case PROVIDER_WRITE:
	adv.set_free(params.get<0>());
	update(retry_count);
	break;
    case PROVIDER_READ:
	break;
    default:
	ERROR("Unknown hook type: " << name);
    }
    INFO("free space has changed, now is: {" << params.get<0>() << "} (FSC)");
}

void pmgr_listener::update(unsigned int retry_count) {
    if (update_rate == adv.get_update_rate()) {
	update_rate = 0;
	rpcvector_t params;
	params.push_back(buffer_wrapper(adv, true));
	publisher->dispatch(publisher_host, publisher_service, PUBLISHER_UPDATE, params, 
			    boost::bind(&pmgr_listener::provider_callback, this, retry_count, _1, _2));
    } else
	update_rate++;
}

void pmgr_listener::timeout_callback(unsigned int retry_count,
				    const boost::system::error_code& error) {
    if (error != boost::asio::error::operation_aborted) {
	rpcvector_t params;
	params.push_back(buffer_wrapper(adv, true));
	publisher->dispatch(publisher_host, publisher_service, PUBLISHER_UPDATE, params, 
			    boost::bind(&pmgr_listener::provider_callback, this, retry_count - 1, _1, _2));
    }
}

void pmgr_listener::provider_callback(unsigned int retry_count,
				     const rpcreturn_t &error, const rpcvector_t &/*buff_result*/) {
    if ((retry_count > 0) && error != rpcstatus::ok) {
	INFO("update callback failure, retrying in " << RETRY_TIMEOUT << "s (" << retry_count << " retries left)");
	timeout_timer->expires_from_now(boost::posix_time::seconds(RETRY_TIMEOUT));
	timeout_timer->async_wait(boost::bind(&pmgr_listener::timeout_callback, this, retry_count, _1));	
    } else {
	INFO("update callback success");
    }
}
