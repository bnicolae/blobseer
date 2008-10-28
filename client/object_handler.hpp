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

    bool create(uint64_t page_size, uint32_t replica_count = 1);
    uint64_t get_latest(uint32_t id = 0);
    bool set_version(unsigned int ver);
    int32_t get_objcount();
    bool read(uint64_t offset, uint64_t size, char *buffer);
    bool write(uint64_t offset, uint64_t size, char *buffer);

    unsigned int get_id() const {
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

    void rpc_provider_callback(buffer_wrapper page_key, interval_range_query::replica_policy_t &repl, 
			       buffer_wrapper buffer, bool &result,
			       const rpcreturn_t &error, const rpcvector_t &val);
};
