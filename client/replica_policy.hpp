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

#ifndef __RANDOM_REPLICA_SELECTION
#define __RANDOM_REPLICA_SELECTION

#include <cstdlib>

#include "common/buffer_wrapper.hpp"
#include "common/structures.hpp"

/// Replica selection policy: random
/**
   This implements a replica selection mechanism for some object. 
   It should encapsulate the version and all replication related
   information. The upper layers will just call try_next(), responsible
   for returning the next replica candidate.
 */
class random_select {
private:
    metadata::provider_list_t providers;
    buffer_wrapper key;
    metadata::provider_desc adv;

public:
    random_select() { }

    bool set_providers(const buffer_wrapper &key_, buffer_wrapper val) {
	key = key_;
	return val.size() != 0 && val.getValue(&providers, true);
    }

    buffer_wrapper get_key() {
	return key;
    }

    metadata::provider_desc try_next() {
	if (providers.size() == 0)
	    return metadata::provider_desc();
	boost::uint32_t rand_idx = rand() % providers.size();
	adv = providers[rand_idx];
	providers.erase(providers.begin() + rand_idx);
	
	return adv;
    }

    metadata::provider_list_t &get_providers() {
	return providers;
    }

    metadata::provider_desc try_again() {
	return adv;
    }
};

#endif
