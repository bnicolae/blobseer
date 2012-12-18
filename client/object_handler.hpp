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

#include <set>

#include <boost/random.hpp>
#include <boost/dynamic_bitset.hpp>

#include "common/config.hpp"
#include "common/structures.hpp"
#include "rpc/rpc_client.hpp"

#include "range_query.hpp"
#include "ft_engine.hpp"

class object_handler {
public: 
    typedef interval_range_query::dht_t dht_t;

    class page_location_t {
    public:
	metadata::provider_desc provider;
	boost::uint64_t offset, size;
	
	page_location_t(metadata::provider_desc &p, boost::uint64_t off, boost::uint64_t s) :
	    provider(p), offset(off), size(s) {}
    };
    typedef std::vector<page_location_t> page_locations_t;

    object_handler(const std::string &config_file);
    ~object_handler();

    bool create(boost::uint64_t page_size, boost::uint32_t ft_info = 1);
    bool clone(boost::int32_t id = 0, boost::int32_t version = 0);
    bool get_latest(boost::uint32_t id = 0);

    bool read(boost::uint64_t offset, boost::uint64_t size, char *buffer, 
	      boost::uint32_t version = 0, boost::uint32_t threshold = 0xFFFFFFFF,
	      const blob::prefetch_list_t &prefetch_list = blob::prefetch_list_t());

    bool get_locations(page_locations_t &loc, boost::uint64_t offset, boost::uint64_t size, 
		       boost::uint32_t version = 0);
    boost::uint32_t append(boost::uint64_t size, char *buffer);
    boost::uint32_t write(boost::uint64_t offset, boost::uint64_t size, char *buffer);

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

    boost::uint32_t get_version() const {
	return latest_root.node.version;
    }

    boost::uint64_t get_page_size() const {
	return latest_root.page_size;
    }

    boost::uint32_t get_id() const {
	return id;
    }

protected:
    rpc_client_t *direct_rpc;

    void rpc_result_callback(bool &result, const rpcreturn_t &error, const rpcvector_t &);

private:
    boost::asio::io_service io_service;
    dht_t *dht;
    interval_range_query *query;
    ft_engine_t *ft_engine;
    
    unsigned int id;
    metadata::root_t latest_root;    
    std::string publisher_host, publisher_service, vmgr_host, vmgr_service;
    boost::mt19937 rnd;
    cache_mt<boost::uint32_t, metadata::root_t, boost::mutex> version_cache;
    unsigned int retry_count;
    bool compression, dedup_flag;

    bool get_root(boost::uint32_t version, metadata::root_t &root);

    boost::uint32_t exec_write(boost::uint64_t offset, boost::uint64_t size, 
			       char *buffer, bool append = false);

    void rpc_pagekey_callback(std::vector<bool> &leaf_duplication_flag,
			      unsigned int index,
			      buffer_wrapper key,
			      buffer_wrapper providers);
};
