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

#ifndef __FT_ENGINE
#define __FT_ENGINE

#include "common/buffer_wrapper.hpp"
#include "common/structures.hpp"

#include "rpc/rpc_client.hpp"
#include "replica_policy.hpp"

class ft_engine_t {
protected:
    rpc_client_t *direct_rpc;
    boost::uint64_t chunk_size;
    unsigned int ft_info, retries;

public:
    ft_engine_t(rpc_client_t *rc, unsigned int r) : direct_rpc(rc), retries(r) { }

    virtual void set_params(boost::uint64_t cs, unsigned int fti) {
	chunk_size = cs;
	ft_info = fti;
    }

    virtual unsigned int get_groups(unsigned int no_chunks) = 0;
    virtual unsigned int get_group_size(unsigned int no_chunks) = 0;

    virtual bool write_chunks(const metadata::query_t &range, metadata::provider_list_t &adv,
			      std::vector<buffer_wrapper> &keys, 
			      std::vector<buffer_wrapper> &contents) = 0;
    virtual void enqueue_read_chunk(random_select &policy, boost::uint64_t offset, buffer_wrapper where, bool &result) = 0;
};

#endif
