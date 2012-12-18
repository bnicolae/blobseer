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

#ifndef __FH_MAP
#define __FH_MAP

#include <fuse_lowlevel.h>

#include <boost/thread/mutex.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>

class fh_map_t {
public:
    struct fh_map_entry_t {
	boost::uint64_t id;
	fuse_ino_t ino;
	unsigned int count;
	
	fh_map_entry_t(boost::uint64_t _id, fuse_ino_t _ino): id(_id), ino(_ino), count(1) { }
    };

    fh_map_t() { }
    ~fh_map_t() { }

    bool update_inode(boost::uint64_t id, fuse_ino_t new_ino);
    boost::uint64_t get_instance(fuse_ino_t ino);
    bool release_instance(boost::uint64_t id);
    void add_instance(boost::uint64_t new_id, fuse_ino_t new_ino);

private:
    // define the tags and the multi-index
    struct tid {};
    struct tino {};

    typedef boost::multi_index_container<
	fh_map_entry_t,
	boost::multi_index::indexed_by <
	    boost::multi_index::hashed_unique<
		boost::multi_index::tag<tid>, BOOST_MULTI_INDEX_MEMBER(fh_map_entry_t, 
								       boost::uint64_t, id)
		>,
	    boost::multi_index::hashed_unique<
		boost::multi_index::tag<tino>, BOOST_MULTI_INDEX_MEMBER(fh_map_entry_t, 
									fuse_ino_t, ino)
		> 
	    >
	> fh_table_t;
    
    typedef boost::multi_index::index<fh_table_t, tid>::type fh_table_by_id;
    typedef boost::multi_index::index<fh_table_t, tino>::type fh_table_by_ino;

    fh_table_t fh_table;
    boost::mutex fh_table_lock;
};

#endif
