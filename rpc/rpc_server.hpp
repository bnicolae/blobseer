#ifndef __RPC_SERVER
#define __RPC_SERVER

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <sstream>

#include "common/null_lock.hpp"
#include "rpc/rpc_meta.hpp"
#include "rpc/rpc_timer.hpp"

#include "common/debug.hpp"

/// RPC server class
/**
   This class implements the server side RPC mechanism. The user hooks an instance of it on his/her 
   io_service, registers RPC callbacks, starts listening asynchronously, then executes the io_service to process 
   requests. All operations: DNS resolution, communication and processing are asynchronous and are chained automatically.
 */
template <class SocketType>
class rpc_server {
private:
    typedef typename SocketType::endpoint endp_t;
    static const unsigned int MAX_RPC_NO = 1024;
    static const unsigned int TIMEOUT = 30;

public:
    /// Creates a RPC server instance
    /**
       Will create a rpc server instance which will hook itself to the supplied io_service.
       @param io_sevice The io_service to hook
     */
    rpc_server(boost::asio::io_service &io_service);

    /// Returns the listening address pretty formatted
    /**
       Returns a string to the listening address representation
       @return The listening address
     */
    const std::string &pretty_format_str() const {
	return descriptor_str;
    }

    /// Destructor
    ~rpc_server();

    /// Registers a RPC handler with the server
    /**
       The RPC to be registered is identified by its id. Each time a client calls this RPC the provided callback 
       runs in the thread running the io_service. See rpc_meta for possible callback types.
       @param name ID of the RPC
       @param version Version of the RPC
       @param param_no Number of parameters to expect on the socket
       @param callback Arguments are: vector of parameters, vector of results and sender id. May miss some of these.
     */
    void register_rpc(boost::uint32_t name, const callback_t &callback) {
	lookup.write(name, callback);
    }
    
    /// Starts listening for incomming connections on a specific interface
    /** 
	Registers DNS resolving for the provided interface, then registers the acceptor for incomming connection.
	Running the io_service blocks current thread for RPC processing.
	@param host The interface to listen to (currently IP address)
	@param service Service of the interface to listen to (currently port)
     */
    void start_listening(const std::string &host, const std::string &service);

    /// Starts listening for incomming connections on custom endpoint
    /** 
	Registers the acceptor for incomming connections. Running the io_service blocks current thread for RPC processing.
	More generic than its other overload, allows the user to manually specify an enpoint (for example to listen on all
	interfaces on TCP IPV4 the custom endpoint might be boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port))
	@param end The boost endpoint to be used for listening
     */
    void start_listening(const endp_t &end);

private:
    typedef rpcinfo_t<SocketType> rpcserver_info_t;
    typedef boost::shared_ptr<rpcserver_info_t> prpcinfo_t;
    typedef cache_mt<unsigned int, callback_t, null_lock, boost::hash<unsigned int>, 
		     cache_mt_none<unsigned int> > lookup_t;

    typedef cached_resolver<SocketType, null_lock> cached_resolver_t;
    typedef boost::shared_ptr<rpc_sync_socket<SocketType> > psocket_t;
    typedef timer_queue_t<psocket_t> timers_t;

    lookup_t lookup;
    typename SocketType::acceptor acceptor;
    cached_resolver_t resolver;
    timers_t timer_queue;
    std::string descriptor_str;

    std::deque<prpcinfo_t> task_queue;
    boost::mutex task_queue_lock;
    boost::condition task_queue_cond;
    boost::thread processor;

    void processor_exec();

    void start_accept();

    void handle_resolve(const boost::system::error_code &error, endp_t end);

    void handle_accept(prpcinfo_t rpc_data, const boost::system::error_code& error);

    void handle_connection(prpcinfo_t rpc_data);

    void handle_header(prpcinfo_t rpc_data, 
		       const boost::system::error_code& error, 
		       size_t bytes_transferred);

    void handle_params(prpcinfo_t rpc_data, unsigned int index);

    void handle_param_size(prpcinfo_t rpc_data, unsigned int index, 
			   const boost::system::error_code& error, 
			   size_t bytes_transferred);

    void handle_param_buffer(prpcinfo_t rpc_data, unsigned int index, char *t, 
			     const boost::system::error_code& error, 
			     size_t bytes_transferred);

    void handle_answer(prpcinfo_t rpc_data, unsigned int index, 
		       const boost::system::error_code& error, 
		       size_t bytes_transferred);

    void handle_answer_size(prpcinfo_t rpc_data, unsigned int index, 
			    const boost::system::error_code& error,
			    size_t bytes_transferred);

    void handle_answer_buffer(prpcinfo_t rpc_data, unsigned int index, 
			      const boost::system::error_code& error,
			      size_t bytes_transferred);
};

template <class SocketType>
void rpc_server<SocketType>::processor_exec() {
    prpcinfo_t rpc_data;

    while (1) {
	// Explicit block to specify lock scope
	{
	    boost::mutex::scoped_lock lock(task_queue_lock);
	    if (task_queue.empty())
		task_queue_cond.wait(lock);
	    rpc_data = task_queue.front();
	    task_queue.pop_front();
	}
	// Explicit block to specify uninterruptible execution scope
	{
	    boost::this_thread::disable_interruption di;
	    rpc_data->result.clear();
	    rpc_data->header.status = boost::apply_visitor(*rpc_data, rpc_data->callback);
	    rpc_data->header.psize = rpc_data->result.size();
	    rpc_data->socket->async_write(boost::asio::buffer((char *)&rpc_data->header,
							      sizeof(rpc_data->header)),
					  boost::asio::transfer_all(),
					  boost::bind(&rpc_server<SocketType>::handle_answer,
						      this, rpc_data, 0, _1, _2));
	}
    }
}

template <class SocketType>
rpc_server<SocketType>::rpc_server(boost::asio::io_service &io_service) :
    lookup(MAX_RPC_NO), acceptor(io_service),
    resolver(io_service), descriptor_str("<unbound>"),
    processor(boost::thread(boost::bind(&rpc_server::processor_exec, this))) { }

template <class SocketType>
rpc_server<SocketType>::~rpc_server() {
    processor.interrupt();
    processor.join();
}

template <class SocketType>
void rpc_server<SocketType>::start_listening(const std::string &host, const std::string &service) {
    resolver.dispatch(host, service, boost::bind(&rpc_server<SocketType>::handle_resolve, this, _1, _2));
}

template <class SocketType>
void rpc_server<SocketType>::start_listening(const endp_t &end) {
    acceptor.open(end.protocol());
    // set the descriptor str accordingly
    descriptor_str = end.address().to_string();
    acceptor.set_option(typename SocketType::acceptor::reuse_address(true));
    acceptor.bind(end);
    acceptor.listen();
    start_accept();
}

template <class SocketType>
void rpc_server<SocketType>::handle_resolve(const boost::system::error_code &error, endp_t end) {
    if (error) 
	ERROR("could not resolve listening interface, error is: " << error);
    else 
	start_listening(end);
}

template <class SocketType>
void rpc_server<SocketType>::start_accept() {
    prpcinfo_t rpc_data = prpcinfo_t(new rpcserver_info_t(acceptor.io_service()));
    acceptor.async_accept(rpc_data->socket->socket(), boost::bind(&rpc_server<SocketType>::handle_accept, 
							  this, rpc_data, _1));
}

template <class SocketType>
void rpc_server<SocketType>::handle_accept(prpcinfo_t rpc_data, const boost::system::error_code& error) {
    start_accept();
    if (error)
	ERROR("could not accept new connection, error is: " << error);
    else {
	DBG("new socket opened");
	handle_connection(rpc_data);
    }
}

template <class SocketType>
void rpc_server<SocketType>::handle_connection(prpcinfo_t rpc_data) {
    timer_queue.add_timer(rpc_data->socket, 
			  boost::posix_time::microsec_clock::local_time() + boost::posix_time::seconds(TIMEOUT));
    rpc_data->socket->async_read(boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
				 boost::asio::transfer_all(),
				 boost::bind(&rpc_server<SocketType>::handle_header, this, rpc_data, _1, _2));
}

template <class SocketType>
void rpc_server<SocketType>::handle_header(prpcinfo_t rpc_data, 
						const boost::system::error_code& error, 
						size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header)) {
	timer_queue.cancel_timer(rpc_data->socket);
	DBG("socket closed");
	return;
    }
    DBG("got new rpc header: " << rpc_data->header.name << " " << rpc_data->header.psize << " " << rpc_data->header.status);
    if (!lookup.read(rpc_data->header.name, &rpc_data->callback)) {
	timer_queue.cancel_timer(rpc_data->socket);
	ERROR("invalid RPC requested: " << rpc_data->header.name);
	return;
    }
    rpc_data->params.resize(rpc_data->header.psize);
    handle_params(rpc_data, 0);
}

template <class SocketType>
void rpc_server<SocketType>::handle_params(prpcinfo_t rpc_data, unsigned int index) {
    if (index < rpc_data->params.size()) {
	rpc_data->socket->async_read(boost::asio::buffer((char *)&rpc_data->header.psize, 
							 sizeof(rpc_data->header.psize)),
				     boost::asio::transfer_all(),
				     boost::bind(&rpc_server<SocketType>::handle_param_size, 
						 this, rpc_data, index, _1, _2));
	return;
    } else {
	boost::mutex::scoped_lock lock(task_queue_lock);
	task_queue.push_back(rpc_data);
	task_queue_cond.notify_one();
    }
}

template <class SocketType>
void rpc_server<SocketType>::handle_param_size(prpcinfo_t rpc_data, unsigned int index,
						    const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	timer_queue.cancel_timer(rpc_data->socket);
	ERROR("could not receive RPC parameter size " <<  index <<  ", error is " << error);
	return;
    }
    DBG("param size = " << rpc_data->header.psize);
    char *t = new char[rpc_data->header.psize];
    rpc_data->socket->async_read(boost::asio::buffer(t, rpc_data->header.psize),
				 boost::asio::transfer_all(), 
				 boost::bind(&rpc_server<SocketType>::handle_param_buffer, 
					     this, rpc_data, index, t, _1, _2));
}

template <class SocketType>
void rpc_server<SocketType>::handle_param_buffer(prpcinfo_t rpc_data, unsigned int index, char *t,
					       const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != rpc_data->header.psize) {
	delete []t;
	timer_queue.cancel_timer(rpc_data->socket);
	ERROR("could not receive RPC parameter buffer " <<  index <<  ", error is " << error);
	return;	
    }
    rpc_data->params[index] = buffer_wrapper(t, rpc_data->header.psize);
    handle_params(rpc_data, index + 1);
}

template <class SocketType>
void rpc_server<SocketType>::handle_answer(prpcinfo_t rpc_data, unsigned int index, 
						const boost::system::error_code& error, size_t bytes_transferred) {
    if (index == 0 && (error || bytes_transferred != sizeof(rpc_data->header))) {
	timer_queue.cancel_timer(rpc_data->socket);
	ERROR("could not send RPC result size, error = " << error);
	return;
    }
    if (index < rpc_data->result.size()) {
	rpc_data->header.psize = rpc_data->result[index].size();
	rpc_data->socket->async_write(boost::asio::buffer((char *)&rpc_data->header.psize, 
							  sizeof(rpc_data->header.psize)),
				      boost::asio::transfer_all(),
				      boost::bind(&rpc_server<SocketType>::handle_answer_size, 
					     this, rpc_data, index, _1, _2));
    } else {
	timer_queue.cancel_timer(rpc_data->socket);
	handle_connection(rpc_data);
    }
}

template <class SocketType>
void rpc_server<SocketType>::handle_answer_size(prpcinfo_t rpc_data, unsigned int index,
						     const boost::system::error_code& error, 
						     size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	timer_queue.cancel_timer(rpc_data->socket);
	ERROR("could not send RPC result buffer size, index = " << index);
	return;
    }
    rpc_data->socket->async_write(boost::asio::buffer(rpc_data->result[index].get(), 
						      rpc_data->result[index].size()),
				  boost::asio::transfer_all(),
				  boost::bind(&rpc_server<SocketType>::handle_answer_buffer,
					      this, rpc_data, index, _1, _2));
}

template <class SocketType>
void rpc_server<SocketType>::handle_answer_buffer(prpcinfo_t rpc_data, unsigned int index,
						       const boost::system::error_code& error, 
						       size_t bytes_transferred) {
    if (!error && bytes_transferred == rpc_data->result[index].size()) {
	handle_answer(rpc_data, index + 1, error, sizeof(rpc_data->header.psize));
    } else {
	timer_queue.cancel_timer(rpc_data->socket);
	ERROR("could not send RPC result buffer, index = " << index);
    }
}

#endif
