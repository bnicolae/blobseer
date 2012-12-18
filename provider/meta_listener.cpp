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

#include "meta_listener.hpp"
#include "common/debug.hpp"

meta_listener::meta_listener() {
}

meta_listener::~meta_listener() {        
}

/*
  index of fields for monitored_params_t:
  0: free space
  1: key
  2: value
  3: sender
*/
void meta_listener::update_event(const boost::int32_t name, monitored_params_t &params) {
    metadata::dhtnode_t meta_node(false);
    
    switch (name) {    
    case PROVIDER_READ:
	params.get<2>().getValue(&meta_node, true);
	meta_node.access_count++;
	params.get<2>().setValue(meta_node, true);
	break;
    case PROVIDER_WRITE:
	params.get<2>().getValue(&meta_node, true);
	meta_node.access_count = 0;
	params.get<2>().setValue(meta_node, true);
	break;
    case PROVIDER_PROBE:
	break;
    default:
	ERROR("Unknown hook type: " << name);
    }
}
