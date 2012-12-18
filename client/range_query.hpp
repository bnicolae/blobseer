/**
 * Copyright (C) 2008-2012 Bogdan Nicolae <bogdan.nicolae@inria.fr>
 *
 * This file is part of BlobSeer, a scalable distributed big data
 * store. Please see README (root of repository) for more details.
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses>.
 */

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
