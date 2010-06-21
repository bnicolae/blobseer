/*
  FUSE local caching support.

  Inodes are of the form:
  |...blob_id...|...version...|
  |<---32bit--->|<---32bit--->|
*/

#define FUSE_USE_VERSION 28

#include <fuse_lowlevel.h>
#include <cstdlib>
#include <cstring>
#include <cerrno>

#include <sstream>

#include "blob_ioctl.hpp"

#include "client/object_handler.cpp"
#include "object_pool.hpp"
#include "local_mirror.hpp"

//#define __DEBUG
#include "common/debug.hpp"

static std::string blobseer_cfg_file = "NONE";

typedef object_pool_t<object_handler> oh_pool_t;
typedef local_mirror_t<oh_pool_t::pobject_t> blob_mirror_t;

static oh_pool_t::pobject_t obj_generator() {
    return oh_pool_t::pobject_t(new object_handler(blobseer_cfg_file));
}

static oh_pool_t *oh_pool;

#define ino_id(ino) ((ino) >> 32)
#define ino_version(ino) ((ino) & 0x00000000FFFFFFFF) 
#define build_ino(id, version) (((boost::uint64_t)(id) << 32) + (version))

void blob_init(char *cfg_file) {
    blobseer_cfg_file = std::string(cfg_file);
    oh_pool = new oh_pool_t(obj_generator);
}

void blob_destroy() {
    delete oh_pool;
}

static int blob_stat(fuse_ino_t ino, struct stat *stbuf) {
    stbuf->st_ino = ino;

    int result = EPROTO;
    oh_pool_t::pobject_t stat_handler = oh_pool->acquire();
    if (!stat_handler)
	return EMFILE; 

    DBG("ino = " << ino << ", id = " << ino_id(ino) << ", ver = " << ino_version(ino));
    if (ino == 1) {
        // root directory
	boost::int32_t no_obj = stat_handler->get_objcount();
	if (no_obj != -1) {
	    stbuf->st_mode = S_IFDIR | 0755;
	    stbuf->st_nlink = no_obj + 2;
	    result = 0;
	}
    } else if (ino_id(ino) != 0 && 
	       ino_version(ino) == 0 &&
	       stat_handler->get_latest(ino_id(ino))) {
	// blob directory, should list all available versions
	stbuf->st_mode = S_IFDIR | 0755;
	stbuf->st_nlink = stat_handler->get_version() + 2;
	result = 0;
    } else if (ino_id(ino) != 0 && 
	       ino_version(ino) != 0 &&
	       stat_handler->get_latest(ino_id(ino)))  {
	// blob version file entry
	stbuf->st_size = stat_handler->get_size(ino_version(ino));
	stbuf->st_mode = S_IFREG | 0666;
	stbuf->st_nlink = 1;
	result = 0;
    }
    
    oh_pool->release(stat_handler);
    return result;
}

void blob_ll_getattr(fuse_req_t req, fuse_ino_t ino,
		       struct fuse_file_info */*fi*/) {
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(stbuf));

    int result = blob_stat(ino, &stbuf);
    if (result != 0)
	fuse_reply_err(req, ENOENT);
    else
	fuse_reply_attr(req, &stbuf, 1.0);
}

void blob_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, 
		     int to_set, struct fuse_file_info */*fi*/) {
    // no change for dirs allowed, nor change of size for files
    DBG("attr mask = " << to_set);
    if (ino_id(ino) == 0 || ino_version(ino) == 0 || ((to_set & FUSE_SET_ATTR_SIZE) != 0))
	fuse_reply_err(req, EPERM);
    else
	fuse_reply_attr(req, attr, 1.0);	
}

void blob_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    struct fuse_entry_param e;

    boost::uint32_t id = ino_id(parent), ver = 0;

    DBG("lookup name = " << name);
    if ((parent == 1 && sscanf(name, "blob-%d", &id) != 1) ||
	(ino_id(parent) != 0 && ino_version(parent) == 0 && 
	 sscanf(name, "version-%d", &ver) != 1) ||
	(ino_id(parent) != 0 && ino_version(parent) != 0))
	fuse_reply_err(req, ENOENT);
    else {
	memset(&e, 0, sizeof(e));
	e.ino = build_ino(id, ver);
	e.attr_timeout = 1.0;
	e.entry_timeout = 1.0;
	blob_stat(e.ino, &e.attr);
	fuse_reply_entry(req, &e);
    }
}

struct dirbuf {
    char *p;
    size_t size;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf *b, const char *name,
		fuse_ino_t ino) {
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
    b->p = (char *) realloc(b->p, b->size);
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf,
		      b->size);
}

static int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
		      off_t off, size_t maxsize) {
    if (off < (off_t)bufsize)
	return fuse_reply_buf(req, buf + off,
			      min(bufsize - off, maxsize));
    else
	return fuse_reply_buf(req, NULL, 0);
}

void blob_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
		       off_t off, struct fuse_file_info */*fi*/) {
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(stbuf));

    int result = blob_stat(ino, &stbuf);
    if (result != 0) {
	fuse_reply_err(req, result);
	return;
    }
    if ((stbuf.st_mode & S_IFDIR) == 0) {
	fuse_reply_err(req, ENOTDIR);
	return;
    }

    DBG("read_dir ino = " << ino);
    dirbuf b;
    memset(&b, 0, sizeof(b));
    dirbuf_add(req, &b, ".", ino);
    dirbuf_add(req, &b, "..", 1);

    if (ino == 1)
	// root directory
	for (unsigned int i = 2; i < stbuf.st_nlink; i++) {
	    ostringstream s;
	    s << "blob-" << (i - 1);
	    dirbuf_add(req, &b, s.str().c_str(), build_ino(i - 1, 0));
	}
    else
	// blob id
	for (unsigned int i = 2; i < stbuf.st_nlink; i++) {
	    ostringstream s("version-");
	    s << "version-" << (i - 1);
	    dirbuf_add(req, &b, s.str().c_str(), build_ino(ino_id(ino), i - 1));
	}

    reply_buf_limited(req, b.p, b.size, off, size);
    free(b.p);
}

void blob_ll_open(fuse_req_t req, fuse_ino_t ino,
		    struct fuse_file_info *fi) {
    if (ino_id(ino) == 0 || ino_version(ino) == 0) {
	fuse_reply_err(req, EISDIR);
	return;
    }

    oh_pool_t::pobject_t blob_handler = oh_pool->acquire();
    if (!blob_handler) {
	fuse_reply_err(req, EMFILE);
	return; 
    }
    if (!blob_handler->get_latest(ino_id(ino))) {
	oh_pool->release(blob_handler);
	fuse_reply_err(req, EIO);
	return;
    }
    blob_mirror_t *lm = NULL;
    fi->fh = (boost::uint64_t)NULL;
    try {
	lm = new blob_mirror_t(blob_handler, ino_version(ino));
	fi->fh = (boost::uint64_t)lm;
    } catch(std::exception &e) {
	ERROR(e.what());
	oh_pool->release(blob_handler);
	delete lm;
	fuse_reply_err(req, EIO);
	return;
    }
    fuse_reply_open(req, fi);
}

void blob_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size,
		    off_t off, struct fuse_file_info *fi) {
    blob_mirror_t *lm = (blob_mirror_t *)fi->fh;
    char *buf;

    boost::uint64_t read_size = lm->read(size, off, buf);
    fuse_reply_buf(req, buf, read_size);
}

void blob_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, 
		   size_t size, off_t off, struct fuse_file_info *fi) {
    blob_mirror_t *lm = (blob_mirror_t *)fi->fh;
    if (lm->write(size, off, buf) != size)
	fuse_reply_err(req, EIO);
    else
	fuse_reply_write(req, size);
}

void blob_ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    blob_mirror_t *lm = (blob_mirror_t *)fi->fh;
    fuse_reply_err(req, lm->flush());
}

void blob_ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    blob_mirror_t *lm = (blob_mirror_t *)fi->fh;

    if (lm) {
	oh_pool_t::pobject_t blob_handler = lm->get_object();
	delete lm;
	oh_pool->release(blob_handler);
    }
}

void blob_ll_ioctl(fuse_req_t req, fuse_ino_t ino, int cmd, 
		   void *arg, struct fuse_file_info *fi, unsigned flagsp, 
		   const void *in_buf, size_t in_bufsz, size_t out_bufszp) {
    blob_mirror_t *lm = (blob_mirror_t *)fi->fh;

    switch (cmd) {
    case COMMIT:	
	fuse_reply_ioctl(req, lm->commit() ? 0 : -1, NULL, 0);
	break;
    case CLONE_AND_COMMIT:
	fuse_reply_ioctl(req, lm->clone_and_commit() ? 0 : -1, NULL, 0);
	break;
    default:
	fuse_reply_err(req, EINVAL);
    }
}
