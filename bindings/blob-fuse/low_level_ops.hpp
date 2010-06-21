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

void blob_ll_ioctl(fuse_req_t req, fuse_ino_t ino, int cmd, 
		   void *arg, struct fuse_file_info *fi, unsigned flagsp, 
		   const void *in_buf, size_t in_bufsz, size_t out_bufszp);
#endif
