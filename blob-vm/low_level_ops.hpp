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

#ifndef __LOW_LEVEL_OPS
#define __LOW_LEVEL_OPS

void blob_init(char *cfg_file);

void blob_destroy();

void blob_ll_getattr(fuse_req_t req, fuse_ino_t ino,
		       struct fuse_file_info */*fi*/);

void blob_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, 
		     int to_set, struct fuse_file_info */*fi*/);

void blob_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);

void blob_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
		       off_t off, struct fuse_file_info */*fi*/);

void blob_ll_open(fuse_req_t req, fuse_ino_t ino,
		    struct fuse_file_info *fi);

void blob_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size,
		    off_t off, struct fuse_file_info *fi);

void blob_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, 
		   size_t size, off_t off, struct fuse_file_info *fi);

void blob_ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

void blob_ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

void blob_ll_fsync(fuse_req_t req, fuse_ino_t ino, int /*datasync*/, struct fuse_file_info *fi);

void blob_ll_ioctl(fuse_req_t req, fuse_ino_t ino, int cmd, 
		   void *arg, struct fuse_file_info *fi, unsigned flagsp, 
		   const void *in_buf, size_t in_bufsz, size_t out_bufszp);
#endif
