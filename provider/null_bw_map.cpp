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

#include "null_bw_map.hpp"

#include <cstdlib>

#include "common/debug.hpp"

null_bw_map::null_bw_map(boost::uint64_t cache_size, boost::uint64_t m) :
    buffer_wrapper_cache(new cache_t(cache_size)), space_left(m) { }

null_bw_map::~null_bw_map() {
    delete buffer_wrapper_cache;
}

boost::uint64_t null_bw_map::get_free() {
    return space_left;
}

bool null_bw_map::read(const buffer_wrapper &key, buffer_wrapper *value) {
    return buffer_wrapper_cache->read(key, value);
}

bool null_bw_map::find(const buffer_wrapper &key) {
    return buffer_wrapper_cache->find(key);
}

bool null_bw_map::write(const buffer_wrapper &key, const buffer_wrapper &value) {
    if (value.size() > space_left)
	return false;
    if (buffer_wrapper_cache->max_size() == buffer_wrapper_cache->size())
	buffer_wrapper_cache->resize(buffer_wrapper_cache->size() + 1);
    buffer_wrapper_cache->write(key, value);
    space_left -= value.size();
    return true;
}
