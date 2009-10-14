#include <boost/random.hpp>

#include "common/config.hpp"
#include "common/structures.hpp"
#include "rpc/rpc_client.hpp"
#include "range_query.hpp"

#include "common/debug.hpp"

class object_handler {
public: 
    typedef interval_range_query::dht_t dht_t;
    typedef rpc_client<config::socket_namespace> rpc_client_t;

    class page_location_t {
    public:
	std::string host;
	std::string port;
	boost::uint64_t offset, size;
	
	page_location_t(const std::string &h, const std::string &p, boost::uint64_t off, boost::uint64_t s) :
	    host(h), port(p), offset(off), size(s) {}
    };
    typedef std::vector<page_location_t> page_locations_t;

    object_handler(const std::string &config_file);
    ~object_handler();

    bool create(boost::uint64_t page_size, boost::uint32_t replica_count = 1);
    bool get_latest(boost::uint32_t id = 0);

    bool read(boost::uint64_t offset, boost::uint64_t size, char *buffer, boost::uint32_t version = 0);
    bool get_locations(page_locations_t &loc, boost::uint64_t offset, boost::uint64_t size, boost::uint32_t version = 0);
    bool append(boost::uint64_t size, char *buffer);
    bool write(boost::uint64_t offset, boost::uint64_t size, char *buffer);

    boost::int32_t get_objcount() const;

    boost::uint64_t get_size(boost::uint32_t version = 0) {
	if (version == 0)
	    return latest_root.current_size;

	metadata::root_t sroot(0, 0, 0, 0, 0);
	if (get_root(version, sroot))
	    return sroot.current_size;
	else
	    return 0;	    
    }

    boost::uint64_t get_version() const {
	return latest_root.node.version;
    }

    boost::uint64_t get_page_size() const {
	return latest_root.get_page_size();
    }

    boost::uint32_t get_id() const {
	return id;
    }

private:
    boost::asio::io_service io_service;
    dht_t *dht;
    interval_range_query *query;
    rpc_client_t *direct_rpc;    
    
    unsigned int id;
    metadata::root_t latest_root;    
    std::string publisher_host, publisher_service, vmgr_host, vmgr_service;
    boost::mt19937 rnd;
    cache_mt<boost::uint32_t, metadata::root_t, boost::mutex> version_cache;

    bool get_root(boost::uint32_t version, metadata::root_t &root);

    bool exec_write(boost::uint64_t offset, boost::uint64_t size, char *buffer, bool append = false);

    void rpc_provider_callback(boost::int32_t, buffer_wrapper page_key, interval_range_query::replica_policy_t &repl, 
			       buffer_wrapper buffer, bool &result,
			       const rpcreturn_t &error, const rpcvector_t &val);
};
