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

#ifndef __NULL_BW_MAP
#define __NULL_BW_MAP

#include "common/config.hpp"

/// Buffer wrapper map that offers no persistency
/**
   Implemented on top of cache_mt
*/

class null_bw_map {
    typedef cache_mt<buffer_wrapper, buffer_wrapper, boost::mutex, buffer_wrapper_hash, cache_mt_none<buffer_wrapper, buffer_wrapper_hash> > cache_t;

    cache_t *buffer_wrapper_cache;
    boost::uint64_t space_left;

public:
    null_bw_map(boost::uint64_t cs, boost::uint64_t ts);
    ~null_bw_map();

    bool read(const buffer_wrapper &key, buffer_wrapper *value);
    bool find(const buffer_wrapper &key);
    bool write(const buffer_wrapper &key, const buffer_wrapper &value);
    boost::uint64_t get_free();
};

#endif
