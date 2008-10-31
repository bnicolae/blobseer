#ifndef __INTERVAL_QUERY
#define __INTERVAL_QUERY

#include <deque>
#include <vector>

#include "common/structures.hpp"
#include "common/simple_dht.hpp"
#include "common/cached_dht.hpp"

#include "replica_policy.hpp"

class interval_range_query {
public:
    // typedef cached_dht<async_dht<bamboo_dht>, buffer_wrapper_hash> dht_t;
    // typedef async_dht<bamboo_dht> dht_t;

    typedef cached_dht<simple_dht<config::socket_namespace, config::lock_t>, buffer_wrapper_hash> dht_t;
    typedef std::deque<metadata::query_t> node_deque_t;
    typedef random_select replica_policy_t;

    interval_range_query(dht_t *dht);
    ~interval_range_query();

    bool readRecordLocations(std::vector<replica_policy_t> &leaves, metadata::query_t &range, metadata::root_t &root);
    bool writeRecordLocations(vmgr_reply &mgr_reply, node_deque_t &node_deque, std::vector<provider_adv> &adv);
private:

    dht_t *dht;
};

#endif
