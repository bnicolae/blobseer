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

#define FUSE_USE_VERSION 28

#include <fuse_lowlevel.h>
#include <boost/format.hpp>

#include "blob_ioctl.hpp"
#include "low_level_ops.hpp"

//#define __DEBUG
#include "common/debug.hpp"

using namespace std;

struct blob_params {
    char *cfg_file;
    bool is_help;
};

#define BLOB_OPT(t, p) { t, offsetof(struct blob_params, p), 1 }

static const struct fuse_opt blob_opts[] = {
    BLOB_OPT("-C %s", cfg_file),
    BLOB_OPT("--cfgfile=%s", cfg_file),
    FUSE_OPT_KEY("-h", 0),
    FUSE_OPT_KEY("--help", 0),
    { NULL, 0, 0}
};

static boost::format usage(
    "usage: blob-vm mountpoint [options]\n"
    "\n"
    "BlobSeer options:\n"
    "    --help|-h                                 print this help message\n"
    "    --cfgfile=<file_name>|-C <file_name>      configuration file (mandatory)\n"
    "\n"
    "Valid ioctl opcodes:\n"
    "CLONE = 0x%X\n"
    "COMMIT = 0x%X\n"
    "MIGRATE = 0x%X\n"
    "\n"
);

static int blob_process_arg(void *data, const char */*arg*/, int key,
			      fuse_args *outargs) {
    blob_params *param = (blob_params *)data;
    
    switch (key) {
    case 0:
	param->is_help = true;
	usage % CLONE % COMMIT % MIGRATE;
	cerr << usage;
	return fuse_opt_add_arg(outargs, "-ho");
    default:
	return 1;
    }
}

int main(int argc, char *argv[]) {
    fuse_args args = FUSE_ARGS_INIT(argc, argv);
    fuse_chan *ch;
    blob_params param = { NULL, false };

    char *mountpoint = NULL;
    int err = -1;

    if (fuse_opt_parse(&args, &param, blob_opts, blob_process_arg) == 0 &&
	!param.is_help) {
	if (param.cfg_file == NULL)
	    cerr << "BlobSeer configuration file is missing." << endl 
		 << "Use -h or --help to display usage." << endl;
	else
	    blob_init(param.cfg_file);
    }

    if ((param.is_help || param.cfg_file != NULL) &&
	fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1 &&
	(ch = fuse_mount(mountpoint, &args)) != NULL) {

	fuse_lowlevel_ops blob_ll_oper;
	memset(&blob_ll_oper, 0, sizeof(blob_ll_oper));
	blob_ll_oper.lookup = blob_ll_lookup;
	blob_ll_oper.getattr = blob_ll_getattr;
	blob_ll_oper.setattr = blob_ll_setattr;
	blob_ll_oper.readdir = blob_ll_readdir;
	blob_ll_oper.open = blob_ll_open;
	blob_ll_oper.read = blob_ll_read;
	blob_ll_oper.write = blob_ll_write;
	blob_ll_oper.flush = blob_ll_flush;
	blob_ll_oper.release = blob_ll_release;
	blob_ll_oper.fsync = blob_ll_fsync;
	blob_ll_oper.ioctl = blob_ll_ioctl;

	fuse_session *se;
	se = fuse_lowlevel_new(&args, &blob_ll_oper,
			       sizeof(blob_ll_oper), NULL);
	if (se != NULL) {
	    if (fuse_set_signal_handlers(se) != -1) {
		fuse_session_add_chan(se, ch);
		DBG("starting FUSE session loop...");
		err = fuse_session_loop(se);
		DBG("FUSE session loop terminated with error code: " << err);		
		fuse_remove_signal_handlers(se);
		fuse_session_remove_chan(ch);
	    }
	    fuse_session_destroy(se);
	}
	fuse_unmount(mountpoint, ch);
    }
    fuse_opt_free_args(&args);
    blob_destroy();

    return err ? 1 : 0;
}
