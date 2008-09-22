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
   requests. All operations: DNS solving, communication and processing are asynchronous and are chained automatically.
 */
template <class Transport, class Lock>
class rpc_server {
private:
    static const unsigned int MAX_RPC_NO = 1024;
public:
    rpc_server(boost::asio::io_service &io_service, unsigned int max_rpc = MAX_RPC_NO) :
	lookup(new lookup_t(max_rpc)), acceptor(new config::socket_namespace::acceptor(io_service)),
	descriptor(new cached_resolver_t(io_service)) { }

    std::string getAddressRepresentation() const {
	std::ostringstream ss;
	ss << *descriptor;
	return ss.str();
    }

    ~rpc_server();

    /// Registers an RPC handler with the server
    /**
       The RPC to be registered is identified by name, version and number of parameters. Each time a client calls
       this RPC the provided callback runs in the thread running the io_service.
       @param name ID of the RPC
       @param version Version of the RPC
       @param param_no Number of parameters to expect on the socket
       @param callback See rpc_server_ts::rpc_server_callback_t for prototype. Arguments are: error code (0 is success) and vector of parameters.       
     */
    void register_rpc(uint32_t name, rpcserver_callback_t callback) {
	lookup->write(name, callback);
    }
    
    /// Starts listening for incomming connections
    /** 
	Registers DNS resolving for the provided interface, then registers acceptors for incomming connection
	Running the io_service blocks current thread for RPC processing
	@param host Host name of the interface to listen to
	@param service Service of the interfae to listen to (currently port)
     */
    void start_listening(const std::string &host, const std::string &service);

private:
    typedef rpcinfo_t<rpcserver_callback_t> rpcserver_info_t;
    typedef boost::shared_ptr<rpcserver_info_t> prpcinfo_t;
    typedef boost::shared_ptr<config::socket_namespace::socket> psocket_t;
    typedef cache_mt<unsigned int, rpcserver_callback_t, Lock, __gnu_cxx::hash<unsigned int>, cache_mt_none<unsigned int> > lookup_t;
    typedef cached_resolver<Transport, Lock> cached_resolver_t;

    lookup_t *lookup;
    config::socket_namespace::acceptor *acceptor;
    cached_resolver_t *descriptor;

    void start_accept();

    void handle_resolve(boost::shared_ptr<cached_resolver_t> resolver, 
			const boost::system::error_code &error, 
			config::socket_namespace::endpoint end);

    void handle_accept(psocket_t socket, const boost::system::error_code& error);

    void handle_connection(psocket_t socket);

    void handle_header(psocket_t socket, prpcinfo_t rpc_data, 
		       const boost::system::error_code& error, 
		       size_t bytes_transferred);

    void handle_params(psocket_t socket, prpcinfo_t rpc_data, unsigned int index);

    void handle_param_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
			   const boost::system::error_code& error, 
			   size_t bytes_transferred);

    void handle_param_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, char *t, 
			     const boost::system::error_code& error, 
			     size_t bytes_transferred);

    void handle_answer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
		       const boost::system::error_code& error, 
		       size_t bytes_transferred);

    void handle_answer_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
			    const boost::system::error_code& error,
			    size_t bytes_transferred);

    void handle_answer_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
			      const boost::system::error_code& error,
			      size_t bytes_transferred);
};

template <class Transport, class Lock>
rpc_server<Transport, Lock>::~rpc_server() {
    delete lookup;
    delete acceptor;
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::start_listening(const std::string &host, const std::string &service) {
    // we make a shared_ptr out of it because the resolver needs to live up until handle resolve is called
    boost::shared_ptr<cached_resolver_t> resolver(new cached_resolver_t(acceptor->io_service()));
    resolver->dispatch(host, service, boost::bind(&rpc_server<Transport, Lock>::handle_resolve, this, resolver, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_resolve(boost::shared_ptr<cached_resolver_t> /*resolver*/, const boost::system::error_code &error, 
						 config::socket_namespace::endpoint end) {
    if (error) 
	ERROR("could not resolve listening interface, error is: " << error);
    else {
	acceptor->open(end.protocol());
	acceptor->set_option(config::socket_namespace::acceptor::reuse_address(true));
	acceptor->bind(end);	
	acceptor->listen();
	start_accept();
    }
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::start_accept() {
    psocket_t socket = psocket_t(new config::socket_namespace::socket(acceptor->io_service()));
    acceptor->async_accept(*socket, boost::bind(&rpc_server<Transport, Lock>::handle_accept, this, socket, _1));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_accept(psocket_t socket, const boost::system::error_code& error) {
    if (error)
	ERROR("could not accept new connection, error is: " << error);
    else
	handle_connection(socket);
    start_accept();	
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_connection(psocket_t socket) {
    prpcinfo_t rpc_data = prpcinfo_t(new rpcserver_info_t());
    boost::asio::async_read(*socket, boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			    boost::asio::transfer_all(),
			    boost::bind(&rpc_server<Transport, Lock>::handle_header, this, socket, rpc_data, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_header(psocket_t socket, prpcinfo_t rpc_data, 
			     const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header)) {
	ERROR("could not read RPC header, error is: " << error);
	return;	
    }
    if (!lookup->read(rpc_data->header.name, &rpc_data->callback)) {
	ERROR("Invalid RPC requested: " << rpc_data->header.name);
	return;	
    }
    rpc_data->params.resize(rpc_data->header.psize);
    handle_params(socket, rpc_data, 0);
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_params(psocket_t socket, prpcinfo_t rpc_data, unsigned int index) {
    if (index < rpc_data->params.size()) {
	boost::asio::async_read(*socket, boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				boost::asio::transfer_all(),
				boost::bind(&rpc_server<Transport, Lock>::handle_param_size, this, socket, rpc_data, index, _1, _2));
	return;
    }
    rpc_data->header.status = rpc_data->callback(rpc_data->params, rpc_data->result);
    if (rpc_data->header.status != rpcstatus::ok)
	rpc_data->result.clear();
    rpc_data->header.psize = rpc_data->result.size();
    boost::asio::async_write(*socket, boost::asio::buffer((char *)&rpc_data->header, sizeof(rpc_data->header)),
			     boost::asio::transfer_all(),
			     boost::bind(&rpc_server<Transport, Lock>::handle_answer, this, socket, rpc_data, 0, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_param_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index,
						    const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	ERROR("Could not receive RPC parameter size " <<  index <<  ", error is " << error);
	return;	
    }
    char *t = new char[rpc_data->header.psize];
    boost::asio::async_read(*socket, boost::asio::buffer(t, rpc_data->header.psize), 
			    boost::asio::transfer_all(), 
			    boost::bind(&rpc_server<Transport, Lock>::handle_param_buffer, this, socket, rpc_data, index, t, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_param_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, char *t,
					       const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != rpc_data->header.psize) {
	delete []t;
	ERROR("Could not receive RPC parameter buffer " <<  index <<  ", error is " << error);
	return;	
    }
    rpc_data->params[index] = buffer_wrapper(t, rpc_data->header.psize);
    handle_params(socket, rpc_data, index + 1);
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_answer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index, 
						const boost::system::error_code& error, size_t bytes_transferred) {
    if (index == 0 && (error || bytes_transferred != sizeof(rpc_data->header))) {
	ERROR("Could not send RPC result size, error = " << error);
	return;
    }
    if (index < rpc_data->result.size()) {
	rpc_data->header.psize = rpc_data->result[index].size();
	boost::asio::async_write(*socket, boost::asio::buffer((char *)&rpc_data->header.psize, sizeof(rpc_data->header.psize)),
				 boost::asio::transfer_all(),
				 boost::bind(&rpc_server<Transport, Lock>::handle_answer_size, this, socket, rpc_data, index, _1, _2));
    }
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_answer_size(psocket_t socket, prpcinfo_t rpc_data, unsigned int index,
						     const boost::system::error_code& error, size_t bytes_transferred) {
    if (error || bytes_transferred != sizeof(rpc_data->header.psize)) {
	ERROR("Could not send RPC result buffer size, index = " << index);
	return;	
    }
    boost::asio::async_write(*socket, boost::asio::buffer(rpc_data->result[index].get(), rpc_data->result[index].size()),
			     boost::asio::transfer_all(), 
			     boost::bind(&rpc_server<Transport, Lock>::handle_answer_buffer, this, socket, rpc_data, index, _1, _2));
}

template <class Transport, class Lock>
void rpc_server<Transport, Lock>::handle_answer_buffer(psocket_t socket, prpcinfo_t rpc_data, unsigned int index,
						       const boost::system::error_code& error, size_t bytes_transferred) {
    if (!error && bytes_transferred == rpc_data->result[index].size()) {
	DBG("Successful RPC reply");
	handle_answer(socket, rpc_data, index + 1, error, sizeof(rpc_data->header.psize));
    } else {
	ERROR("Could not send RPC result buffer, index = " << index);
    }
}

#endif
