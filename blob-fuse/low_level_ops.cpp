#define FUSE_USE_VERSION 28

#include <fuse_lowlevel.h>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <sstream>
#include <boost/algorithm/string/trim.hpp>

#include "libconfig.h++"

#include "blob_ioctl.hpp"
#include "inode_util.hpp"

#include "migration.hpp"
#include "object_pool.hpp"
#include "local_mirror.hpp"
#include "fh_map.hpp"

//#define __DEBUG
#include "common/debug.hpp"

static std::string blobseer_cfg_file = "NONE", migr_svc = "NONE";
static bool writes_mirror_flag, migr_push_flag, migr_push_all_flag;

typedef object_pool_t<migration_wrapper> oh_pool_t;
typedef local_mirror_t<oh_pool_t::pobject_t> blob_mirror_t;

static oh_pool_t::pobject_t obj_generator() {
    return oh_pool_t::pobject_t(new migration_wrapper(blobseer_cfg_file));
}

static oh_pool_t *oh_pool;
static fh_map_t fh_map;

void blob_init(char *cfg_file) {
    blobseer_cfg_file = std::string(cfg_file);
    oh_pool = new oh_pool_t(obj_generator);
    
    libconfig::Config cfg;
    try {
        cfg.readFile(blobseer_cfg_file.c_str());
	// get dht port
	if (!cfg.lookupValue("fuse.migration_port", migr_svc))
	    FATAL("listening port for migration requests missing");
	if (!cfg.lookupValue("fuse.mirgation_mirror_writes", writes_mirror_flag))
	    FATAL("mirror writes flag for migration requests missing");
	if (!cfg.lookupValue("fuse.mirgation_push_flag", migr_push_flag))
	    FATAL("push flag for migration requests missing");
	if (!cfg.lookupValue("fuse.mirgation_push_all_flag", migr_push_all_flag))
	    FATAL("push all flag for migration requests missing");
    } catch(libconfig::FileIOException) {
	FATAL("I/O error trying to parse config file: " + blobseer_cfg_file);
    } catch(libconfig::ParseException &e) {
	std::ostringstream ss;
	ss << "parse exception for cfg file " << blobseer_cfg_file
	   << "(line " << e.getLine() << "): " << e.getError();
	FATAL(ss.str());
    } catch(std::runtime_error &e) {
	throw e;
    } catch(...) {
	FATAL("unexpected exception");
    }
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
	    std::ostringstream s;
	    s << "blob-" << (i - 1);
	    dirbuf_add(req, &b, s.str().c_str(), build_ino(i - 1, 0));
	}
    else
	// blob id
	for (unsigned int i = 2; i < stbuf.st_nlink; i++) {
	    std::ostringstream s("version-");
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

    fi->fh = fh_map.get_instance(ino);
    if (fi->fh == (boost::uint64_t)NULL) {
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
	try {
	    lm = new blob_mirror_t(&fh_map, blob_handler, ino_version(ino), 
				   migr_svc, writes_mirror_flag,
				   migr_push_flag, migr_push_all_flag);
	    fi->fh = (boost::uint64_t)lm;
	    fh_map.add_instance(fi->fh, ino);
	} catch(std::exception &e) {
	    ERROR(e.what());
	    oh_pool->release(blob_handler);
	    delete lm;
	    fuse_reply_err(req, EIO);
	    return;
	}
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
    if (fi->fh != (boost::uint64_t)NULL && fh_map.release_instance(fi->fh)) {
	blob_mirror_t *lm = (blob_mirror_t *)fi->fh;
	oh_pool_t::pobject_t blob_handler = lm->get_object();
	delete lm;
	oh_pool->release(blob_handler);
    }
}

void blob_ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
    blob_mirror_t *lm = (blob_mirror_t *)fi->fh;
    fuse_reply_err(req, lm->sync());
}

void blob_ll_ioctl(fuse_req_t req, fuse_ino_t ino, int cmd, 
                   void *arg, struct fuse_file_info *fi, unsigned flagsp, 
                   const void *in_buf, size_t in_bufsz, size_t out_bufszp) {
    blob_mirror_t *lm = (blob_mirror_t *)fi->fh;
    std::string str_arg((const char *)in_buf, in_bufsz);
    boost::trim(str_arg);

    switch (cmd) {
    case COMMIT:
        fuse_reply_ioctl(req, lm->commit(), NULL, 0);
        break;
    case CLONE:
        fuse_reply_ioctl(req, lm->clone(), NULL, 0);
        break;
    case MIGRATE:
	fuse_reply_ioctl(req, lm->migrate_to(str_arg), NULL, 0);
	break;
    default:
        fuse_reply_err(req, EINVAL);
    }
}
