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

#ifndef __INODE
#define __INODE

/*
Inodes are of the form:
|...blob_id...|...version...|
|<---12bit--->|<---20bit--->|
*/

#define VERSION_SIZE 20

#define ino_id(ino) ((ino) >> VERSION_SIZE)
#define ino_version(ino) ((ino) & (((unsigned)1 << VERSION_SIZE) - 1))
#define build_ino(id, version) (((id) << VERSION_SIZE) + (version))

#endif
