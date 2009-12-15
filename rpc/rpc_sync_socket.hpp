#ifndef __RPC_SYNC_SOCKET
#define __RPC_SYNC_SOCKET

/**
   Synchronized asynchronous asio sockets. Can be shared among threads.
 */
template <class Socket> class rpc_sync_socket {
private:
    typedef typename Socket::socket socket_t;
    typedef boost::shared_ptr<socket_t> psocket_t;
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
    void async_read(const MutableBufferSequence & buffers, CompletionCondition completion_condition, ReadHandler handler) {
	scoped_lock lock(mutex);
	boost::asio::async_read(socket_, buffers, completion_condition, handler);
    }
    
    template<typename ConstBufferSequence, typename CompletionCondition, typename WriteHandler>
    void async_write(const ConstBufferSequence & buffers, CompletionCondition completion_condition, WriteHandler handler) {
	scoped_lock lock(mutex);
	boost::asio::async_write(socket_, buffers, completion_condition, handler);
    }
    
    template<typename EndpointType, typename ConnectHandler>
    void async_connect(const EndpointType &peer_endpoint, ConnectHandler handler) {
	scoped_lock lock(mutex);
	socket_.async_connect(peer_endpoint, handler);
    }
    
private:
    socket_t socket_;
    boost::mutex mutex;
};

template <class Socket>
void rpc_sync_socket<Socket>::close() {
    scoped_lock lock(mutex);
    socket_.close();
}

#endif
