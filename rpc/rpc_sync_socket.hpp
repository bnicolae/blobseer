#ifndef __RPC_SYNC_SOCKET
#define __RPC_SYNC_SOCKET

/**
   Synchronized asynchronous asio sockets. Can be shared among threads.
 */
template <class Socket> class rpc_sync_socket {
private:
    typedef typename Socket::socket socket_t;
    typedef boost::mutex::scoped_lock scoped_lock;
           
public:
    rpc_sync_socket(boost::asio::io_service &io) : socket_(io) { }
    ~rpc_sync_socket() { 
	close();
    }

    socket_t &socket() {
	return socket_;
    }
    bool is_open() const {
	return socket_.is_open();
    }
    void close();

    template<typename MutableBufferSequence, typename CompletionCondition, typename ReadHandler>
    void async_read(const MutableBufferSequence & buffers,
		    CompletionCondition completion_condition, ReadHandler handler) {
	scoped_lock lock(mutex);
	try {
	    boost::asio::async_read(socket_, buffers, completion_condition, handler);
	} catch(...) {
	    socket_.get_io_service().post(boost::bind(handler, 
						      boost::asio::error::bad_descriptor, 0));
	}
    }
    
    template<typename ConstBufferSequence, typename CompletionCondition, typename WriteHandler>
    void async_write(const ConstBufferSequence & buffers, 
		     CompletionCondition completion_condition, WriteHandler handler) {
	scoped_lock lock(mutex);
	try {
	    boost::asio::async_write(socket_, buffers, completion_condition, handler);
	} catch(...) {
	    socket_.get_io_service().post(boost::bind(handler, 
						      boost::asio::error::bad_descriptor, 0));
	}
    }
    
    template<typename EndpointType, typename ConnectHandler>
    void async_connect(const EndpointType &peer_endpoint, ConnectHandler handler) {
	scoped_lock lock(mutex);
	try {
	    socket_.async_connect(peer_endpoint, handler);
	} catch (...) {
	    socket_.get_io_service().post(boost::bind(handler, 
						      boost::asio::error::bad_descriptor));
	}
    }
    
private:
    socket_t socket_;
    boost::mutex mutex;
};

template <class Socket>
void rpc_sync_socket<Socket>::close() {
    scoped_lock lock(mutex);
    try {
	socket_.close();
    } catch (std::exception &e) {
	ERROR("could not close socket, error is: " << e.what());
    }
}

#endif
