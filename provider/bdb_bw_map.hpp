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

#ifndef __BDB_BW_MAP
#define __BDB_BW_MAP

#include <boost/thread/mutex.hpp>
#include <db_cxx.h>

#include "common/config.hpp"

/// Thread safe buffer wrapper persistency layer
/**
   Implemented on top of cache_mt and Berkley DB
*/

class bdb_bw_map {
    typedef cache_mt<buffer_wrapper, buffer_wrapper, boost::mutex, 
		     buffer_wrapper_hash, cache_mt_LRU<buffer_wrapper, buffer_wrapper_hash> > cache_t;
    typedef boost::mutex::scoped_lock scoped_lock;
    typedef std::pair<buffer_wrapper, buffer_wrapper> write_entry_t;

    static const unsigned int MAX_WRITE_QUEUE = 1024;

    cache_t buffer_wrapper_cache;
    Db *db; 
    DbEnv db_env;
    boost::uint64_t space_left;
    std::deque<write_entry_t> write_queue;
    boost::mutex write_queue_lock;
    boost::condition write_queue_cond;
    boost::thread process_writes;
    
public:
    bdb_bw_map(const std::string &db_name, boost::uint64_t cs, boost::uint64_t ts);
    ~bdb_bw_map();

    bool read(const buffer_wrapper &key, buffer_wrapper *value);
    bool write(const buffer_wrapper &key, const buffer_wrapper &value);
    bool find(const buffer_wrapper &key);
    void evict(const buffer_wrapper &key, const buffer_wrapper &value);
    boost::uint64_t get_free();
    void write_exec();
};

#endif
