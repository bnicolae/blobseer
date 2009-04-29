#ifndef __BDB_BW_MAP
#define __BDB_BW_MAP

#include <boost/thread/mutex.hpp>
#include <db_cxx.h>

#include "common/config.hpp"

/// Thread safe buffer wrapper persistency layer
/**
   Implemented on top of cache_mt and Berkley DB
*/

class bdb_bw_map {
    typedef cache_mt<buffer_wrapper, buffer_wrapper, boost::mutex, buffer_wrapper_hash, cache_mt_LRU<buffer_wrapper, buffer_wrapper_hash> > cache_t;

    cache_t *buffer_wrapper_cache;
    Db *db;
    DbEnv *db_env;
    boost::thread sync_thread;
    boost::uint64_t space_left;
    unsigned int sync_timeout;

public:
    bdb_bw_map(const std::string &db_name, boost::uint64_t cs, boost::uint64_t ts, unsigned int to);
    ~bdb_bw_map();

    bool read(const buffer_wrapper &key, buffer_wrapper *value);
    bool write(const buffer_wrapper &key, const buffer_wrapper &value);
    boost::uint64_t get_free();
    void sync_handler();
};

#endif
