#include "common/config.hpp"
#include "common/structures.hpp"
#include "rpc/rpc_client.hpp"
#include "range_query.hpp"

class object_handler {
public: 
    typedef interval_range_query::dht_t dht_t;
    typedef rpc_client<config::socket_namespace, config::lock_t> rpc_client_t;

    object_handler(const std::string &config_file);
    ~object_handler();

    bool create(boost::uint64_t page_size, boost::uint32_t replica_count = 1);
    bool get_latest(boost::uint32_t id = 0, boost::uint64_t *size = NULL);
    bool set_version(unsigned int ver);

    bool read(boost::uint64_t offset, boost::uint64_t size, char *buffer);
    bool append(boost::uint64_t size, char *buffer);
    bool write(boost::uint64_t offset, boost::uint64_t size, char *buffer);

    boost::int32_t get_objcount() const;

    boost::uint64_t get_size() const {
	return latest_root.current_size;
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
    std::string publisher_host, publisher_service, lockmgr_host, lockmgr_service;

    bool exec_write(boost::uint64_t offset, boost::uint64_t size, char *buffer, bool append = false);

    void rpc_provider_callback(buffer_wrapper page_key, interval_range_query::replica_policy_t &repl, 
			       buffer_wrapper buffer, bool &result,
			       const rpcreturn_t &error, const rpcvector_t &val);
};
