#ifndef __INTERVAL_QUERY
#define __INTERVAL_QUERY

#include <deque>
#include <vector>
#include <map>

#include "common/structures.hpp"
#include "common/simple_dht.hpp"
#include "common/cached_dht.hpp"

#include "replica_policy.hpp"

namespace blob {
    // entries in the prefetch queue are of the form (access count, offset)
    typedef std::pair<boost::uint64_t, boost::uint32_t> prefetch_entry_t;
    // entries are stored in a std::set
    typedef std::map<boost::uint64_t, boost::uint32_t> prefetch_list_t;
}

class interval_range_query {
public:
    typedef cached_dht<simple_dht<config::socket_namespace>, buffer_wrapper_hash> dht_t;
    typedef std::deque<metadata::query_t> node_deque_t;
    typedef random_select replica_policy_t;

    interval_range_query(dht_t *dht);
    ~interval_range_query();

    bool readRecordLocations(std::vector<replica_policy_t> &leaves, 
			     blob::prefetch_list_t &prefetch_list,
			     metadata::query_t &range, metadata::root_t &root,
			     boost::uint32_t threshold);

    bool writeRecordLocations(vmgr_reply &mgr_reply, 
			      std::vector<buffer_wrapper> &leaf_keys,
			      std::vector<bool> &leaf_duplication_flag,
			      unsigned int group_size,
			      metadata::provider_list_t &provider_list);
private:
    dht_t *dht;

};

#endif
