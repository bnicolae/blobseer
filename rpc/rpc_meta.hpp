#ifndef __RPC_META
#define __RPC_META

#include <deque>
#include <boost/variant.hpp>

// Basic types
typedef std::vector<buffer_wrapper> rpcvector_t;
typedef boost::shared_ptr<rpcvector_t> prpcvector_t;
namespace rpcstatus {
    typedef enum {ok, earg, eres, eobj, egen} rpcreturn_t;
}
typedef rpcstatus::rpcreturn_t rpcreturn_t;

// Callbacks
typedef boost::function<void (const rpcreturn_t &, const rpcvector_t &) > rpcclient_callback_t;
typedef boost::function<rpcreturn_t (const rpcvector_t &, rpcvector_t &)> rpcserver_callback_t;
typedef boost::function<rpcreturn_t (const rpcvector_t &, rpcvector_t &, const std::string &sender)> rpcserver_extcallback_t;

typedef boost::variant<rpcclient_callback_t, rpcserver_callback_t, rpcserver_extcallback_t> callback_t;

/// RPC header: RPC name (id), number of parameters, status code
class rpcheader_t {
public:
    uint32_t name, psize;
    int32_t status;
    
    rpcheader_t(uint32_t n, uint32_t s) : name(n), psize(s), status(rpcstatus::ok) { }
};

template<class Transport> class rpcinfo_t : public boost::static_visitor<rpcstatus::rpcreturn_t>, private boost::noncopyable {
public:
    typedef typename Transport::socket socket_t;
    typedef boost::shared_ptr<socket_t> psocket_t;

    unsigned int id;
    string_pair_t host_id;
    rpcvector_t params;
    rpcvector_t result;
    rpcheader_t header;
    psocket_t socket;
    callback_t callback;
        
    rpcinfo_t(boost::asio::io_service &io) : header(rpcheader_t(0, 0)), socket(new socket_t(io)) { }
    template<class Callback> rpcinfo_t(const std::string &h, const std::string &s,
				       uint32_t n, const rpcvector_t &p, 
				       Callback c, const rpcvector_t &r) : 
	host_id(string_pair_t(h, s)), params(p), result(r), header(rpcheader_t(n, p.size())), callback(c), id(0) { }
	
    void assign_id(const unsigned int request_id) {
	id = request_id;
    }
    
    rpcstatus::rpcreturn_t operator()(const rpcclient_callback_t &cb) {
	cb(static_cast<rpcreturn_t>(header.status), result);
	return rpcstatus::ok;
    }

    rpcstatus::rpcreturn_t operator()(const rpcserver_callback_t &cb) {
	return cb(params, result);
    }
    
    rpcstatus::rpcreturn_t operator()(const rpcserver_extcallback_t &cb) {
	return cb(params, result, socket->remote_endpoint().address().to_string());
    }
};

#endif
