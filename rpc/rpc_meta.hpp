#ifndef __RPC_META
#define __RPC_META

#include <deque>

typedef std::vector<buffer_wrapper> rpcvector_t;
typedef boost::shared_ptr<rpcvector_t> prpcvector_t;
namespace rpcstatus {
    typedef enum {ok, earg, eres, eobj, egen} rpcreturn_t;
}
typedef rpcstatus::rpcreturn_t rpcreturn_t;
typedef boost::function<void (const rpcreturn_t &, const rpcvector_t &) > rpcclient_callback_t;
typedef boost::function<rpcreturn_t (const rpcvector_t &, rpcvector_t &)> rpcserver_callback_t;

template <class Callback>
class rpcinfo_t {
public:
    typedef Callback callback_t;

    class rpcheader_t {
    public:
	uint32_t name;
	int32_t status; 
	uint32_t psize;

	rpcheader_t(uint32_t n, uint32_t s) : name(n), status(rpcstatus::ok), psize(s) { }
    };

    std::string host, service;
    rpcvector_t params;
    rpcvector_t result;
    rpcheader_t header;
    Callback callback;

    rpcinfo_t() : header(rpcheader_t(0, 0)) { }

    rpcinfo_t(const std::string &h, const std::string &s,
	      uint32_t n, const rpcvector_t &p, 
	      Callback c, const rpcvector_t &r) : 
	host(h), service(s), params(p), result(r), header(rpcheader_t(n, p.size())), callback(c)  { }
};

template<class RPCInfo>
class rpcbundle_t {
    typedef boost::shared_ptr<RPCInfo> prpcinfo_t;

    string_pair_t host_id;
    std::deque<RPCInfo> rpc_queue;
public:
    rpcbundle_t(const string_pair_t &id) {
	this->host_id = id;
    }
    const string_pair_t &get_id() {
	return host_id;
    }
    void enqueue_rpc(uint32_t name, uint32_t version, const rpcvector_t &params, 
		     typename RPCInfo::callback_t callback, buffer_wrapper result) {
	prpcinfo_t entry(new RPCInfo(host_id.first, host_id.second, name, version, params, callback, result));
	rpc_queue.push_back(entry);	
    }
    prpcinfo_t dequeue_rpc() {
	if (rpc_queue.size() > 0) {
	    prpcinfo_t result = rpc_queue.front();
	    rpc_queue.pop_front();
	    return result;
	} else
	    return prpcinfo_t();
    }
};

#endif
