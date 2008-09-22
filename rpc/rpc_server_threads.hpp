#ifndef __RPC_SERVER
#define __RPC_SERVER

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/threadpool.hpp>
#include <sstream>

#include "common/cache_mt.hpp"
#include "common/buffer_wrapper.hpp"
#include "common/addr_desc.hpp"

class rpc_server {
    static const unsigned int max_rpc_proc_no = 1024;
    typedef boost::asio::ip::SOCK_TYPE socket_namespace;
    typedef boost::function<buffer_wrapper (const boost::system::error_code &, const std::vector<buffer_wrapper> &)> callback_type;
    typedef boost::shared_ptr<socket_namespace::socket> psocket_type;
    typedef std::pair<unsigned int, callback_type> value_type;
    typedef cache_mt<unsigned int, value_type, __gnu_cxx::hash<unsigned int>, cache_mt_none<unsigned int> > lookup_type;
    typedef boost::threadpool::fifo_pool thread_pool_type;
    
public:
    rpc_server(const std::string &host, const std::string &service, unsigned int pool_size, unsigned int max_rpc = max_rpc_proc_no);

    std::string getAddressRepresentation() const {
	std::ostringstream ss;
	ss << *descriptor;
	return ss.str();
    }

    ~rpc_server() {
	delete this->lookup;
	delete this->tp;
	delete this->descriptor;
    }

    void register_rpc(uint32_t name, uint32_t version, unsigned int param_no, callback_type callback) {
	lookup->write(getProcID(name, version), value_type(param_no, callback));
    }

    void run();

private:
    thread_pool_type *tp;
    lookup_type *lookup;
    addr_desc<socket_namespace> *descriptor;

    int getProcID(uint32_t name, uint32_t version) {
	return name << 10 + version;
    }

    void exec_rpc(psocket_type socket);    
};

#endif
