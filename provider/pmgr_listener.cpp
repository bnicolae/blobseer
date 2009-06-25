#include "pmgr_listener.hpp"
#include "common/debug.hpp"

pmgr_listener::pmgr_listener(boost::asio::io_service &io_service,
			     const std::string &ph, const std::string &ps,
			     const boost::uint64_t fs,
			     const std::string &s)
    : phost(ph), pservice(ps),
      service(s),
      free_space(fs),
      publisher(io_service), 
      timeout_timer(io_service) 
{
    timeout_callback(boost::system::error_code());
}

pmgr_listener::~pmgr_listener() {        
}

void pmgr_listener::update_event(const boost::int32_t name, const monitored_params_t &params) {
    switch (name) {    
    case PROVIDER_WRITE:
	free_space = params.get<0>();
	INFO("write_page initiated by " << params.get<3>() << ", page size is: {" << params.get<2>() << "} (WPS)");
	INFO("free space has changed, now is: {" << params.get<0>() << "} (FSC)");
	break;
    case PROVIDER_READ:
	INFO("read_page initiated by " << params.get<3>() << ", page size is: {" << params.get<2>() << "} (RPS)");
	break;
    default:
	ERROR("Unknown hook type: " << name);
    }
}

void pmgr_listener::timeout_callback(const boost::system::error_code& error) {
    if (error != boost::asio::error::operation_aborted) {
	rpcvector_t params;
	params.push_back(buffer_wrapper(free_space, true));
	params.push_back(buffer_wrapper(service, true));
	publisher.dispatch(phost, pservice, PUBLISHER_UPDATE, params, 
			   boost::bind(&pmgr_listener::provider_callback, this, _1, _2));
    }
}

void pmgr_listener::provider_callback(const rpcreturn_t &error, const rpcvector_t &/*buff_result*/) {
    if (error != rpcstatus::ok)
	INFO("update callback failure, retrying in " << UPDATE_TIMEOUT << "s");
    timeout_timer.expires_from_now(boost::posix_time::seconds(UPDATE_TIMEOUT));
    timeout_timer.async_wait(boost::bind(&pmgr_listener::timeout_callback, this, _1));	
}
