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

#ifndef __VMGR
#define __VMGR

#include <boost/cstdint.hpp>

const boost::int32_t VMGR_GETROOT = 1;
const boost::int32_t VMGR_GETTICKET = 2;
const boost::int32_t VMGR_CREATE = 3;
const boost::int32_t VMGR_PUBLISH = 4;
const boost::int32_t VMGR_GETOBJNO = 5;
const boost::int32_t VMGR_CLONE = 6;

#endif
