#ifndef __RPC_CLIENT
#define __RPC_CLIENT

#include <boost/function.hpp>
#include <boost/threadpool.hpp>
#include <boost/asio.hpp>

#include "common/buffer_wrapper.hpp"
#include "common/addr_desc.hpp"
#include "common/cache_mt.hpp"

class rpc_client
{
public:
    typedef boost::asio::ip::SOCK_TYPE socket_namespace;
    typedef std::vector<buffer_wrapper> param_type;     
    typedef boost::threadpool::fifo_pool thread_pool_type;
    typedef boost::function<void (const boost::system::error_code &, buffer_wrapper) > callback_type;
    typedef addr_desc<boost::asio::ip::SOCK_TYPE> addr_desc_type;

    typedef cache_mt<std::string, addr_desc_type> host_cache_type;

    rpc_client(unsigned int pool_size = default_pool_size) {
	this->tp = new thread_pool_type(pool_size);
	this->host_cache = new host_cache_type(pool_size);
    }

    ~rpc_client() {
	delete this->tp;
	delete this->host_cache;
    }
    
    void schedule(const std::string &host, const std::string &service,
		  uint32_t name, uint32_t version, 
		  const param_type &params,
		  callback_type result_callback,
		  buffer_wrapper result = buffer_wrapper()) {
	tp->schedule(boost::bind(&rpc_client::exec_rpc, this, host, service, name, version, boost::cref(params), result_callback, result));	
    }

    void wait() {
	tp->wait();
    }

    template <class T> static void get_serialized_callback(bool &result, T &query, const boost::system::error_code &error, buffer_wrapper val) {	
	result = (!error && val.size() != 0 && val.getValue(&query, true));
    }

private:    
    static const unsigned int default_pool_size = 128;

    void exec_rpc(const std::string &host, const std::string &service,
			 uint32_t name, uint32_t version, 
			 const param_type &params,
			 callback_type result_callback,
			 buffer_wrapper result);

    host_cache_type *host_cache;
    thread_pool_type *tp;    
};

#endif
