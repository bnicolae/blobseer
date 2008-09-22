#ifndef __RPC_CLIENT
#define __RPC_CLIENT

#include <deque>

#include "common/hashers.hpp"
#include "rpc_meta.hpp"

#include "common/debug.hpp"

template <class Transport, class Lock> class rpc_client
{
public:
    rpc_client(boost::asio::io_service &io_service, unsigned int timeout = DEFAULT_TIMEOUT);
    ~rpc_client(); 
    
    void dispatch(const std::string &host, const std::string &service,
		  uint32_t name,
		  const rpcvector_t &params,
		  rpcclient_callback_t result_callback,
		  const rpcvector_t &result);

    void dispatch(const std::string &host, const std::string &service,
		  uint32_t name,
		  const rpcvector_t &params,
		  rpcclient_callback_t result_callback);

    bool run();
    
private:
    typedef rpcinfo_t<rpcclient_callback_t> rpcclient_info_t;
    typedef boost::shared_ptr<rpcclient_info_t> prpcinfo_t;
    typedef boost::shared_ptr<typename Transport::socket> psocket_t;
    typedef std::deque<prpcinfo_t> request_queue_t;
    typedef cached_resolver<Transport, Lock> host_cache_t;
    
    static const unsigned int DEFAULT_TIMEOUT = 5;    
    // the system caps the number of max opened sockets, let's use 256 for now
    static const unsigned int WAIT_LIMIT = 256;

    host_cache_t *host_cache;
    unsigned int waiting_count, timeout;
    boost::asio::io_service *io_service;
    request_queue_t request_queue;

    void handle_connect(psocket_t socket, prpcinfo_t rpc_data, const boost::system::error_code& error);

    void handle_next_request();

    void handle_resolve(prpcinfo_t rpc_data, 
			const boost::system::error_code& error, typename Transport::endpoint end);

    void handle_header(psocket_t socket, prpcinfo_t rpc_data, 
		       const boost::system::error_code& error, size_t bytes_transferred);

    void handle_callback(prpcinfo_t rpc_data, const boost::system::error_code &error);

    void handle_params(psocket_t socket, prpcinfo_t rpc_data, unsigned int index);

    void handle_param_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
			   const boost::system::error_code& error, size_t bytes_transferred);

    void handle_param_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
			     const boost::system::error_code& error, size_t bytes_transferred);

    void handle_answer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
		       const boost::system::error_code& error, size_t bytes_transferred);
    
    void handle_answer_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
			    const boost::system::error_code& error, 
			    size_t bytes_transferred);

    void handle_answer_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
			      const boost::system::error_code& error, 
			      size_t bytes_transferred);

    void on_timeout(const boost::system::error_code& error);
};

template <class Transport, class Lock>
rpc_client<Transport, Lock>::rpc_client(boost::asio::io_service &io_service, unsigned int timeout) {
    this->host_cache = new host_cache_t(io_service);
    this->io_service = &io_service;
    this->waiting_count = 0;
    this->timeout = timeout;
}

template <class Transport, class Lock>
rpc_client<Transport, Lock>::~rpc_client() {
    delete this->host_cache;
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::dispatch(const std::string &host, const std::string &service,
	      uint32_t name,
	      const rpcvector_t &params,
	      rpcclient_callback_t callback,
	      const rpcvector_t &result) {
    request_queue.push_back(prpcinfo_t(new rpcclient_info_t(host, service, name, params, callback, result)));
    DBG("RPC request enqueued (" << name << "), results will be managed by the client");
    handle_next_request();
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::dispatch(const std::string &host, const std::string &service,
	      uint32_t name,
	      const rpcvector_t &params,
	      rpcclient_callback_t callback) {
    request_queue.push_back(prpcinfo_t(new rpcclient_info_t(host, service, name, params, callback, rpcvector_t())));
    DBG("RPC request enqueued (" << name << "), results will be managed automatically");
    handle_next_request();
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_next_request() {
    if (!request_queue.empty() && waiting_count < WAIT_LIMIT) {
	prpcinfo_t rpc_data = request_queue.front();
	request_queue.pop_front();
	waiting_count++;
	host_cache->dispatch(boost::ref(rpc_data->host), boost::ref(rpc_data->service), 
			     boost::bind(&rpc_client<Transport, Lock>::handle_resolve, this, rpc_data, _1, _2));
    }
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_resolve(prpcinfo_t rpc_data, const boost::system::error_code& error, typename Transport::endpoint end) {
    if (error) {
	handle_callback(rpc_data, error);
	return;
    }
    psocket_t socket = psocket_t(new typename Transport::socket(*io_service));
    socket->async_connect(end, boost::bind(&rpc_client<Transport, Lock>::handle_connect, this, socket, rpc_data, _1));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_connect(psocket_t socket, prpcinfo_t rpc_data, const boost::system::error_code& error) {
    if (error) {
	handle_callback(rpc_data, error);
	return;
    }
    boost::asio::async_write(*socket, boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			     boost::asio::transfer_all(),
			     boost::bind(&rpc_client<Transport, Lock>::handle_header, this, socket, rpc_data, _1, _2));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_header(psocket_t socket, prpcinfo_t rpc_data, 
						const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header)) {
	handle_callback(rpc_data, error);
	return;
    }
    handle_params(socket, rpc_data, 0);
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_params(psocket_t socket, prpcinfo_t rpc_data, unsigned int index) {
    if (index < rpc_data->params.size()) {
	rpc_data->header.psize = rpc_data->params[index].size();
	boost::asio::async_write(*socket, boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				 boost::asio::transfer_all(),
				 boost::bind(&rpc_client<Transport, Lock>::handle_param_size, this, socket, rpc_data, index, _1, _2));
	return;
    }
    boost::asio::async_read(*socket, boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			    boost::asio::transfer_all(),
			    boost::bind(&rpc_client<Transport, Lock>::handle_answer, this, socket, rpc_data, 0, _1, _2));    
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_param_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index,
						    const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	handle_callback(rpc_data, error);
	return;	
    }
    boost::asio::async_write(*socket, boost::asio::buffer(rpc_data->params[index].get(), rpc_data->params[index].size()),
			     boost::asio::transfer_all(),
			     boost::bind(&rpc_client<Transport, Lock>::handle_param_buffer, this, socket, rpc_data, index, _1, _2));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_param_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index,
			      const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != rpc_data->params[index].size()) {
	handle_callback(rpc_data, boost::asio::error::invalid_argument);
	return;
    }
    handle_params(socket, rpc_data, index + 1);
}


template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_answer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
						const boost::system::error_code& error, size_t bytes_transferred) {
    if (index == 0) {
	if (!error && bytes_transferred == sizeof(rpc_data->header))	
	    rpc_data->result.resize(rpc_data->header.psize);
	else	    
	    handle_callback(rpc_data, boost::asio::error::invalid_argument);
    }
    if (index < rpc_data->result.size()) {	
	boost::asio::async_read(*socket, boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				 boost::asio::transfer_all(),
				 boost::bind(&rpc_client<Transport, Lock>::handle_answer_size, this, socket, rpc_data, index, _1, _2));
	return;
    }
    handle_callback(rpc_data, boost::system::error_code());
}


template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_answer_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index,
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
    boost::asio::async_read(*socket, boost::asio::buffer(rpc_data->result[index].get(), rpc_data->result[index].size()),
			    boost::asio::transfer_all(),
			    boost::bind(&rpc_client<Transport, Lock>::handle_answer_buffer, this, socket, rpc_data, index, _1, _2));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_answer_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index,
						       const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != rpc_data->result[index].size()) {
	handle_callback(rpc_data, boost::asio::error::invalid_argument);
	return;
    }
    handle_answer(socket, rpc_data, index + 1, boost::system::error_code(), sizeof(rpc_data->header.psize));
}

template <class Transport, class Lock>
void rpc_client<Transport, Lock>::handle_callback(prpcinfo_t rpc_data, const boost::system::error_code &error) {
    waiting_count--;
    DBG("ready to run callback and decremented waiting_count: " << waiting_count << ", error: " << error);
    if (error)
	rpc_data->header.status = rpcstatus::egen;
    rpc_data->callback(static_cast<rpcreturn_t>(rpc_data->header.status), rpc_data->result);
    handle_next_request();
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
