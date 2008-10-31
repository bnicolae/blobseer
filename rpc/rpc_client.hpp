#ifndef __RPC_CLIENT
#define __RPC_CLIENT

#include <deque>

#include "common/hashers.hpp"
#include "rpc_meta.hpp"

#include "common/debug.hpp"

/// Threaded safe per host buffering for RPC calls
/**
   Dispatches go through the RPC request buffer. The main idea is to enqueue them
   on a per host basis. This way we can transparently execute multiple RPC requests
   for the same server by using a single connection.
 */
template <class Transport, class Lock>
class request_queue_t {
public:
    typedef rpcinfo_t<Transport> request_t;    
    typedef boost::shared_ptr<request_t> prpcinfo_t;

private:
    typedef std::deque<prpcinfo_t> host_queue_t;
    typedef __gnu_cxx::hash_map<string_pair_t, host_queue_t> requests_t;
    typedef typename requests_t::iterator requests_it;
    typedef typename Lock::scoped_lock scoped_lock;

    Lock mutex;
    requests_t request_queue;
public:
    request_queue_t() { }
    void enqueue(prpcinfo_t request);
    void clear();
    prpcinfo_t dequeue(const string_pair_t &host_id);
}; 

template <class Transport, class Lock>
void request_queue_t<Transport, Lock>::enqueue(prpcinfo_t request) { 
    scoped_lock lock(mutex);
    
    requests_it i = request_queue.find(request->host_id);
    if (i == request_queue.end())
	i = request_queue.insert(std::pair<string_pair_t, host_queue_t>(request->host_id, host_queue_t())).first;
    i->second.push_back(request);
}

template <class Transport, class Lock>
typename request_queue_t<Transport, Lock>::prpcinfo_t request_queue_t<Transport, Lock>::dequeue(const string_pair_t &host_id) {
    scoped_lock lock(mutex);

    requests_it i = request_queue.find(host_id);
    if (i == request_queue.end())
	i = request_queue.begin();
    prpcinfo_t result;
    if (i != request_queue.end()) {
	if (i->second.size() > 0) {
	    result = i->second.front();
	    i->second.pop_front();
	}
	if (i->second.size() == 0)
	    request_queue.erase(i);
    }
    return result;
}

template <class Transport, class Lock>
void request_queue_t<Transport, Lock>::clear() { 
    request_queue.clear();
}

/// The RPC client
template <class Transport, class Lock> class rpc_client
{
public:
    /// RPC client constructor 
    /**
       @param io_sevice The io_service to use.
       @param timeout The timeout to wait for an answer on the socket
     */
    rpc_client(boost::asio::io_service &io_service, unsigned int timeout = DEFAULT_TIMEOUT);
    ~rpc_client(); 

    /// Dispatch an RPC request to the io_service. The result is managed by the client directly.
    /**
       The io_service will eventually run the RPC, collect the result and call a specified callback.
       The result will be managed by the client directly. This might be useful if you want to use a 
       custom buffer for results.
       @param host The name of the host
       @param service The name of the service
       @param name The RPC id
       @param params Vector to the params (buffer_wrapped).
       @param result_callback The client side callback
       @param result The client managed result vector (all custom items must be buffer_wrapped)
     */    
    void dispatch(const std::string &host, const std::string &service,
		  uint32_t name,
		  const rpcvector_t &params,
		  rpcclient_callback_t result_callback,
		  const rpcvector_t &result);

    /// Dispatch an RPC request to the io_service. The result is managed by the client directly.
    /**
       The io_service will eventually run the RPC, collect the result and call a specified callback.
       The result will be managed automatically and passed to the callback.
       @param host The name of the host
       @param service The name of the service
       @param name The RPC id
       @param params Vector to the params (buffer_wrapped).
       @param result_callback The client side callback
     */    
    void dispatch(const std::string &host, const std::string &service,
		  uint32_t name,
		  const rpcvector_t &params,
		  rpcclient_callback_t result_callback);

    /// Run the io_service.
    /** 
	Run this instead of the io_service's run, if you intend to wait for RPC calls.
	It will ensure all callbacks will eventually execute, the RPC buffering flushed 
	and the io_service will reset.
    */
    bool run();
    
private:
    typedef request_queue_t<Transport, Lock> requests_t;
    typedef typename requests_t::request_t rpcinfo_t;
    typedef typename requests_t::prpcinfo_t prpcinfo_t;
    typedef boost::shared_ptr<typename Transport::socket> psocket_t;

    typedef cached_resolver<Transport, Lock> host_cache_t;
    
    static const unsigned int DEFAULT_TIMEOUT = 5;    
    // the system caps the number of max opened sockets, let's use 256 for now
    static const unsigned int WAIT_LIMIT = 256;

    host_cache_t *host_cache;
    unsigned int waiting_count, timeout;
    boost::asio::io_service *io_service;
    requests_t request_queue;

    void handle_connect(prpcinfo_t rpc_data, const boost::system::error_code& error);

    void handle_next_request(psocket_t socket, const string_pair_t &host_id);

    void handle_resolve(prpcinfo_t rpc_data, 
			const boost::system::error_code& error, typename Transport::endpoint end);

    void handle_header(prpcinfo_t rpc_data, 
		       const boost::system::error_code& error, size_t bytes_transferred);

    void handle_callback(prpcinfo_t rpc_data, const boost::system::error_code &error);

    void handle_params(prpcinfo_t rpc_data, unsigned int index);

    void handle_param_size(prpcinfo_t rpc_data, unsigned int index, 
			   const boost::system::error_code& error, size_t bytes_transferred);

    void handle_param_buffer(prpcinfo_t rpc_data, unsigned int index, 
			     const boost::system::error_code& error, size_t bytes_transferred);

    void handle_answer(prpcinfo_t rpc_data, unsigned int index, 
		       const boost::system::error_code& error, size_t bytes_transferred);
    
    void handle_answer_size(prpcinfo_t rpc_data, unsigned int index, 
			    const boost::system::error_code& error, 
			    size_t bytes_transferred);

    void handle_answer_buffer(prpcinfo_t rpc_data, unsigned int index, 
			      const boost::system::error_code& error, 
			      size_t bytes_transferred);

    void on_timeout(const boost::system::error_code& error);
};

template <class Transport, class Lock>
rpc_client<Transport, Lock>::rpc_client(boost::asio::io_service &io, unsigned int t) :
    host_cache(new host_cache_t(io)), waiting_count(0), timeout(t), io_service(&io) { }

template <class Transport, class Lock>
rpc_client<Transport, Lock>::~rpc_client() {
    delete host_cache;
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::dispatch(const std::string &host, const std::string &service,
					   uint32_t name,
					   const rpcvector_t &params,
					   rpcclient_callback_t callback,
					   const rpcvector_t &result) {
    prpcinfo_t info(new rpcinfo_t(host, service, name, params, callback, result));
    request_queue.enqueue(info);
    DBG("RPC request enqueued (" << name << "), results will be managed by the client");
    handle_next_request(psocket_t(), info->host_id);
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::dispatch(const std::string &host, const std::string &service,
					   uint32_t name,
					   const rpcvector_t &params,
					   rpcclient_callback_t callback) {
    prpcinfo_t info(new rpcinfo_t(host, service, name, params, callback, rpcvector_t()));
    request_queue.enqueue(info);
    DBG("RPC request enqueued (" << name << "), results will be managed automatically");
    handle_next_request(psocket_t(), info->host_id);
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_next_request(psocket_t socket, const string_pair_t &host_id) {
    prpcinfo_t rpc_data = request_queue.dequeue(host_id);
    if (!rpc_data)
	return;
    if (socket && rpc_data->host_id == host_id) {
	rpc_data->socket = socket;
	waiting_count++;
	handle_connect(rpc_data, boost::system::error_code());
    } else if (waiting_count < WAIT_LIMIT) {
	waiting_count++;
	host_cache->dispatch(boost::ref(rpc_data->host_id.first), boost::ref(rpc_data->host_id.second), 
			     boost::bind(&rpc_client<Transport, Lock>::handle_resolve, this, rpc_data, _1, _2));
    } else
	request_queue.enqueue(rpc_data);
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_resolve(prpcinfo_t rpc_data, const boost::system::error_code& error, typename Transport::endpoint end) {
    if (error) {
	handle_callback(rpc_data, error);
	return;
    }
    rpc_data->socket = psocket_t(new typename Transport::socket(*io_service));
    rpc_data->socket->async_connect(end, boost::bind(&rpc_client<Transport, Lock>::handle_connect, this, rpc_data, _1));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_connect(prpcinfo_t rpc_data, const boost::system::error_code& error) {
    if (error) {
	handle_callback(rpc_data, error);
	return;
    }
    DBG("sending the header: " << rpc_data->header.name << " " << rpc_data->header.psize << " " << rpc_data->header.status << " " << rpc_data->header.keep_alive);
    boost::asio::async_write(*(rpc_data->socket), boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			     boost::asio::transfer_all(),
			     boost::bind(&rpc_client<Transport, Lock>::handle_header, this, rpc_data, _1, _2));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_header(prpcinfo_t rpc_data, 
						const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header)) {
	handle_callback(rpc_data, error);
	return;
    }
    handle_params(rpc_data, 0);
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_params(prpcinfo_t rpc_data, unsigned int index) {
    if (index < rpc_data->params.size()) {
	rpc_data->header.psize = rpc_data->params[index].size();
	boost::asio::async_write(*(rpc_data->socket), boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				 boost::asio::transfer_all(),
				 boost::bind(&rpc_client<Transport, Lock>::handle_param_size, this, rpc_data, index, _1, _2));
	return;
    }
    boost::asio::async_read(*(rpc_data->socket), boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			    boost::asio::transfer_all(),
			    boost::bind(&rpc_client<Transport, Lock>::handle_answer, this, rpc_data, 0, _1, _2));    
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_param_size(prpcinfo_t rpc_data, unsigned int index,
						    const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	handle_callback(rpc_data, error);
	return;	
    }
    boost::asio::async_write(*(rpc_data->socket), boost::asio::buffer(rpc_data->params[index].get(), rpc_data->params[index].size()),
			     boost::asio::transfer_all(),
			     boost::bind(&rpc_client<Transport, Lock>::handle_param_buffer, this, rpc_data, index, _1, _2));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_param_buffer(prpcinfo_t rpc_data, unsigned int index,
						      const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != rpc_data->params[index].size()) {
	handle_callback(rpc_data, boost::asio::error::invalid_argument);
	return;
    }
    handle_params(rpc_data, index + 1);
}


template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_answer(prpcinfo_t rpc_data, unsigned int index, 
						const boost::system::error_code& error, size_t bytes_transferred) {
    if (index == 0) {
	DBG("got back header: " << rpc_data->header.name << " " << rpc_data->header.psize << " " << rpc_data->header.status << " " << rpc_data->header.keep_alive);
	if (!error && bytes_transferred == sizeof(rpc_data->header))	
	    rpc_data->result.resize(rpc_data->header.psize);
	else	    
	    handle_callback(rpc_data, boost::asio::error::invalid_argument);
    }
    if (index < rpc_data->result.size()) {
	boost::asio::async_read(*(rpc_data->socket), boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				 boost::asio::transfer_all(),
				 boost::bind(&rpc_client<Transport, Lock>::handle_answer_size, this, rpc_data, index, _1, _2));
	return;
    }
    handle_callback(rpc_data, boost::system::error_code());
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_answer_size(prpcinfo_t rpc_data, unsigned int index,
						     const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	handle_callback(rpc_data, boost::asio::error::invalid_argument);
	return;	
    }
    bool is_managed = rpc_data->result[index].size() != 0;
    if (rpc_data->header.psize == 0 || (is_managed && rpc_data->result[index].size() != rpc_data->header.psize)) {
	handle_callback(rpc_data, boost::asio::error::invalid_argument);
	return;
    }
    char *result_ptr;
    if (is_managed)
	result_ptr = rpc_data->result[index].get();
    else {
	result_ptr = new char[rpc_data->header.psize];
	rpc_data->result[index] = buffer_wrapper(result_ptr, rpc_data->header.psize);
    }
    boost::asio::async_read(*(rpc_data->socket), boost::asio::buffer(rpc_data->result[index].get(), rpc_data->result[index].size()),
			    boost::asio::transfer_all(),
			    boost::bind(&rpc_client<Transport, Lock>::handle_answer_buffer, this, rpc_data, index, _1, _2));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_answer_buffer(prpcinfo_t rpc_data, unsigned int index,
						       const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != rpc_data->result[index].size()) {
	handle_callback(rpc_data, boost::asio::error::invalid_argument);
	return;
    }
    handle_answer(rpc_data, index + 1, boost::system::error_code(), sizeof(rpc_data->header.psize));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_callback(prpcinfo_t rpc_data, const boost::system::error_code &error) {
    waiting_count--;
    DBG("ready to run callback and decremented waiting_count: " << waiting_count << ", error: " << error);
    if (error)
	rpc_data->header.status = rpcstatus::egen;
    boost::apply_visitor(*rpc_data, rpc_data->callback);
    handle_next_request(rpc_data->socket, rpc_data->host_id);
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::on_timeout(const boost::system::error_code& error) {
    if (error != boost::asio::error::operation_aborted) {
	// Timer was not cancelled, take necessary action.
	INFO("wait timeout(" << timeout << " s), handlers are aborted");
    }
}

template <class Transport, class Lock>
bool rpc_client<Transport, Lock>::run() {
    io_service->run();
    bool result = waiting_count == 0;
    waiting_count = 0;
    request_queue.clear();
    io_service->reset();
    return result;
}

#endif
