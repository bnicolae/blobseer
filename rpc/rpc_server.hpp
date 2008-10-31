#ifndef __RPC_SERVER
#define __RPC_SERVER

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <sstream>

#include "common/debug.hpp"
#include "rpc_meta.hpp"

/// RPC server class
/**
   This class implements the server side RPC mechanism. The user hooks an instance of it on his/her 
   io_service, registers RPC callbacks, starts listening asynchronously, then executes the io_service to process 
   requests. All operations: DNS resolution, communication and processing are asynchronous and are chained automatically.
 */
template <class Transport, class Lock>
class rpc_server {    
private:
    typedef typename Transport::endpoint endp_t;
    static const unsigned int MAX_RPC_NO = 1024;
public:
    /// Creates a RPC server instance
    /**
       Will create a rpc server instance which will hook itself to the supplied io_service.
       @param io_sevice The io_service to hook
     */
    rpc_server(boost::asio::io_service &io_service) :
	lookup(new lookup_t(MAX_RPC_NO)), acceptor(new config::socket_namespace::acceptor(io_service)),
	resolver(new cached_resolver_t(io_service)), descriptor_str("<unbound>") { }

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
    void register_rpc(uint32_t name, const callback_t &callback) {
	lookup->write(name, callback);
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
    typedef rpcinfo_t<Transport> rpcserver_info_t;
    typedef boost::shared_ptr<rpcserver_info_t> prpcinfo_t;
    typedef cache_mt<unsigned int, callback_t, Lock, __gnu_cxx::hash<unsigned int>, cache_mt_none<unsigned int> > lookup_t;
    typedef cached_resolver<Transport, Lock> cached_resolver_t;

    lookup_t *lookup;
    typename Transport::acceptor *acceptor;
    cached_resolver_t *resolver;
    std::string descriptor_str;

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

template <class Transport, class Lock>
rpc_server<Transport, Lock>::~rpc_server() {
    delete lookup;
    delete resolver;
    delete acceptor;
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::start_listening(const std::string &host, const std::string &service) {
    resolver->dispatch(host, service, boost::bind(&rpc_server<Transport, Lock>::handle_resolve, this, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::start_listening(const endp_t &end) {
    acceptor->open(end.protocol());
    // set the descriptor str accordingly
    descriptor_str = end.address().to_string();
    acceptor->set_option(typename Transport::acceptor::reuse_address(true));
    acceptor->bind(end);	
    acceptor->listen();
    start_accept();
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_resolve(const boost::system::error_code &error, endp_t end) {
    if (error) 
	ERROR("could not resolve listening interface, error is: " << error);
    else {
	acceptor->open(end.protocol());
	// set the descriptor str accordingly
	descriptor_str = end.address().to_string();
	acceptor->set_option(typename Transport::acceptor::reuse_address(true));
	acceptor->bind(end);	
	acceptor->listen();
	start_accept();
    }
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::start_accept() {
    prpcinfo_t rpc_data = prpcinfo_t(new rpcserver_info_t(acceptor->io_service()));
    acceptor->async_accept(*rpc_data->socket, boost::bind(&rpc_server<Transport, Lock>::handle_accept, this, rpc_data, _1));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_accept(prpcinfo_t rpc_data, const boost::system::error_code& error) {
    if (error)
	ERROR("could not accept new connection, error is: " << error);
    else
	handle_connection(rpc_data);
    start_accept();	
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_connection(prpcinfo_t rpc_data) {
    boost::asio::async_read(*rpc_data->socket, boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			    boost::asio::transfer_all(),
			    boost::bind(&rpc_server<Transport, Lock>::handle_header, this, rpc_data, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_header(prpcinfo_t rpc_data, const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header)) {
	if (error != boost::asio::error::eof)
	    ERROR("could not read RPC header, error is: " << error);
	return;	
    }
    DBG("got the rpc header: " << rpc_data->header.name << " " << rpc_data->header.psize << " " << rpc_data->header.status << " " << rpc_data->header.keep_alive);
    if (!lookup->read(rpc_data->header.name, &rpc_data->callback)) {
	ERROR("invalid RPC requested: " << rpc_data->header.name);
	return;	
    }
    rpc_data->params.resize(rpc_data->header.psize);
    handle_params(rpc_data, 0);
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_params(prpcinfo_t rpc_data, unsigned int index) {
    if (index < rpc_data->params.size()) {
	boost::asio::async_read(*rpc_data->socket, boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				boost::asio::transfer_all(),
				boost::bind(&rpc_server<Transport, Lock>::handle_param_size, this, rpc_data, index, _1, _2));
	return;
    }
    rpc_data->result.clear();
    rpc_data->header.status = boost::apply_visitor(*rpc_data, rpc_data->callback);
    if (rpc_data->header.status != rpcstatus::ok)
	rpc_data->result.clear();
    rpc_data->header.psize = rpc_data->result.size();
    boost::asio::async_write(*rpc_data->socket, boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			     boost::asio::transfer_all(),
			     boost::bind(&rpc_server<Transport, Lock>::handle_answer, this, rpc_data, 0, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_param_size(prpcinfo_t rpc_data, unsigned int index,
						    const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	ERROR("could not receive RPC parameter size " <<  index <<  ", error is " << error);
	return;	
    }
    char *t = new char[rpc_data->header.psize];
    boost::asio::async_read(*rpc_data->socket, boost::asio::buffer(t, rpc_data->header.psize), 
			    boost::asio::transfer_all(), 
			    boost::bind(&rpc_server<Transport, Lock>::handle_param_buffer, this, rpc_data, index, t, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_param_buffer(prpcinfo_t rpc_data, unsigned int index, char *t,
					       const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != rpc_data->header.psize) {
	delete []t;
	ERROR("could not receive RPC parameter buffer " <<  index <<  ", error is " << error);
	return;	
    }
    rpc_data->params[index] = buffer_wrapper(t, rpc_data->header.psize);
    handle_params(rpc_data, index + 1);
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_answer(prpcinfo_t rpc_data, unsigned int index, 
						const boost::system::error_code& error, size_t bytes_transferred) {
    if (index == 0 && (error || bytes_transferred != sizeof(rpc_data->header))) {
	ERROR("could not send RPC result size, error = " << error);
	return;
    }
    if (index < rpc_data->result.size()) {
	rpc_data->header.psize = rpc_data->result[index].size();
	boost::asio::async_write(*rpc_data->socket, boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				 boost::asio::transfer_all(),
				 boost::bind(&rpc_server<Transport, Lock>::handle_answer_size, this, rpc_data, index, _1, _2));
    } else
	handle_connection(rpc_data);
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_answer_size(prpcinfo_t rpc_data, unsigned int index,
						     const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	ERROR("could not send RPC result buffer size, index = " << index);
	return;	
    }
    boost::asio::async_write(*rpc_data->socket, boost::asio::buffer(rpc_data->result[index].get(), rpc_data->result[index].size()),
			     boost::asio::transfer_all(), 
			     boost::bind(&rpc_server<Transport, Lock>::handle_answer_buffer, this, rpc_data, index, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_answer_buffer(prpcinfo_t rpc_data, unsigned int index,
						       const boost::system::error_code& error, size_t bytes_transferred) {
    if (!error && bytes_transferred == rpc_data->result[index].size()) {
	DBG("successful RPC reply");
	handle_answer(rpc_data, index + 1, error, sizeof(rpc_data->header.psize));
    } else {
	ERROR("could not send RPC result buffer, index = " << index);
    }
}

#endif
