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

#ifndef __PROVIDER
#define __PROVIDER

#include <boost/cstdint.hpp>

const boost::int32_t PROVIDER_READ = 1;
const boost::int32_t PROVIDER_WRITE = 2;
const boost::int32_t PROVIDER_FREE = 3;
const boost::int32_t PROVIDER_READ_PARTIAL = 4;
const boost::int32_t PROVIDER_PROBE = 5;
const boost::int32_t PROVIDER_VERSION = 0;

const boost::int32_t PROVIDER_OK = 0;
const boost::int32_t PROVIDER_EARG = -1;
const boost::int32_t PROVIDER_EMEM = -2;
const boost::int32_t PROVIDER_ENOTFOUND = -3;

#endif
