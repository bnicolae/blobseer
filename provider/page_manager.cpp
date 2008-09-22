#include "page_manager.hpp"
#include "common/debug.hpp"

page_manager::page_manager(boost::asio::io_service &io_service,
			   const provider_adv &a, 
			   const std::string &phost, 
			   const std::string &pservice) : 
    publisher_host(phost), publisher_service(pservice), adv(a)
{
    publisher = new rpc_client_t(io_service);
    timeout_timer = new boost::asio::deadline_timer(io_service);
    page_cache = new page_cache_t(adv.get_free());
    update_rate = adv.get_update_rate();
    update(~(unsigned int)0);
}

page_manager::~page_manager() {    
    delete page_cache;
    delete publisher;
    delete timeout_timer;
}

void page_manager::update(unsigned int retry_count) {
    if (update_rate == adv.get_update_rate()) {
	update_rate = 0;
	rpcvector_t params;
	params.push_back(buffer_wrapper(adv, true));
	publisher->dispatch(publisher_host, publisher_service, PUBLISHER_UPDATE, params, 
			    boost::bind(&page_manager::provider_callback, this, retry_count, _1, _2));
    } else
	update_rate++;
}

void page_manager::timeout_callback(unsigned int retry_count,
				    const boost::system::error_code& error) {
    if (error != boost::asio::error::operation_aborted) {
	rpcvector_t params;
	params.push_back(buffer_wrapper(adv, true));
	publisher->dispatch(publisher_host, publisher_service, PUBLISHER_UPDATE, params, 
			    boost::bind(&page_manager::provider_callback, this, retry_count - 1, _1, _2));
    }
}

void page_manager::provider_callback(unsigned int retry_count,
				     const rpcreturn_t &error, const rpcvector_t &/*buff_result*/) {
    if ((retry_count > 0) && error != rpcstatus::ok) {
	INFO("update callback failure, retrying in " << RETRY_TIMEOUT << "s (" << retry_count << " retries left)");
	timeout_timer->expires_from_now(boost::posix_time::seconds(RETRY_TIMEOUT));
	timeout_timer->async_wait(boost::bind(&page_manager::timeout_callback, this, retry_count, _1));	
    } else {
	INFO("update callback success");
    }
}

rpcreturn_t page_manager::free_page(const rpcvector_t &params, rpcvector_t &/*result*/) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument, required page id");	
	return rpcstatus::earg;
    }
    page_cache->free(params[0]);
    adv.set_free(page_cache->size());
    INFO("RPC successful");
    update();
    return rpcstatus::ok;
}

rpcreturn_t page_manager::write_page(const rpcvector_t &params, rpcvector_t &/*result*/) {
    if (params.size() != 2) {
	ERROR("RPC error: wrong argument, required page id");	
	return rpcstatus::earg;
    }
    if (!page_cache->write(params[0], params[1])) {
	ERROR("memory is full");
	return rpcstatus::eres;
    } else {
	adv.set_free(page_cache->max_size() - page_cache->size());
	update();	
	INFO("RPC successful");
	return rpcstatus::ok;
    }
}

rpcreturn_t page_manager::read_page(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument, required page id");	
	return rpcstatus::earg;
    }
    // code to be executed    
    buffer_wrapper data;
    if (!page_cache->read(params[0], &data)) {
	ERROR("page was not found: " << params[0]);
	return rpcstatus::eobj;
    } else {
	INFO("RPC successful");
	result.push_back(data);
	return rpcstatus::ok;
    }
}
