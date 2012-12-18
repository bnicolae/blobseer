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

#ifndef __VMANAGEMENT
#define __VMANAGEMENT

#include <map>

#include "common/config.hpp"
#include "common/null_lock.hpp"
#include "common/structures.hpp"
#include "rpc/rpc_meta.hpp"

class vmanagement {
public:
    rpcreturn_t get_root(const rpcvector_t &params, rpcvector_t &result,
			 const std::string &sender);
    rpcreturn_t get_ticket(const rpcvector_t &params, rpcvector_t &result,
			   const std::string &sender);
    rpcreturn_t get_objcount(const rpcvector_t &params, rpcvector_t &result,
			     const std::string &sender);
    rpcreturn_t create(const rpcvector_t &params, rpcvector_t &result,
		       const std::string &sender);
    rpcreturn_t publish(const rpcvector_t &params, rpcvector_t &result,
			const std::string &sender);
    rpcreturn_t clone(const rpcvector_t &params, rpcvector_t &result,
			const std::string &sender);

    ~vmanagement();
    vmanagement();
private:
    typedef boost::unordered_map<unsigned int, obj_info, boost::hash<unsigned int> > obj_hash_t;

    void compute_sibling_versions(metadata::siblings_enum_t &siblings,
				  metadata::query_t &edge_node,
				  obj_info::interval_list_t &intervals, 
				  boost::uint64_t root_size);
    obj_hash_t obj_hash;
    boost::int32_t obj_count;

    // all operations are serialized and performed in the same thread.
    typedef null_lock::scoped_lock scoped_lock;
    null_lock mgr_lock;
};

#endif
