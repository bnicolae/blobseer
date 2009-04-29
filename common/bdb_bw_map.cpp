#include "bdb_bw_map.hpp"

#include <cstdlib>

#include "common/debug.hpp"

void *buffer_wrapper_alloc(size_t size) {
    return static_cast<void *>(new char[size]);
}

void buffer_wrapper_free(void *ptr) {
    delete [](static_cast<char *>(ptr));
}

bdb_bw_map::bdb_bw_map(const std::string &db_name, boost::uint64_t cache_size, boost::uint64_t m, unsigned int to) :
    db_env(new DbEnv(0)), buffer_wrapper_cache(new cache_t(cache_size)), space_left(m), sync_timeout(to)
{    
    boost::filesystem::path path(db_name.c_str());
    boost::filesystem::create_directories(path.parent_path());

    DBG("db_name = " << path.filename() << ", path = " << path.parent_path().file_string().c_str());
    db_env->set_alloc(buffer_wrapper_alloc, realloc, buffer_wrapper_free);
    db_env->open(path.parent_path().file_string().c_str(), DB_INIT_CDB | DB_INIT_MPOOL | DB_THREAD | DB_CREATE, 0);
    db = new Db(db_env, 0);
    db->set_error_stream(&std::cerr);
    db->open(NULL, path.filename().c_str(), NULL, DB_HASH, DB_CREATE | DB_THREAD | DB_READ_UNCOMMITTED, 0);

    sync_thread = boost::thread(boost::bind(&bdb_bw_map::sync_handler, this));
}

void bdb_bw_map::sync_handler() {
    boost::xtime xt;

    for (;;) {
	// let's sleep for a while
	boost::xtime_get(&xt, boost::TIME_UTC);
	xt.sec += sync_timeout;
	boost::thread::sleep(xt);

	// now start the DB sync; make this operation uninterruptible
	{
	    boost::this_thread::disable_interruption di;
	    try {
		db->sync(0);
	    } catch (DbException &e) {
		ERROR("sync triggered, but failed: " << e.what());
	    }
	}
    }
}

bdb_bw_map::~bdb_bw_map() {
    delete buffer_wrapper_cache;

    sync_thread.interrupt();
    sync_thread.join();
    db->close(0);
    db_env->close(0);
}

boost::uint64_t bdb_bw_map::get_free() {
    return space_left;
}

bool bdb_bw_map::read(const buffer_wrapper &key, buffer_wrapper *value) {
    if (buffer_wrapper_cache->read(key, value))
	return true;

    Dbt db_key(key.get(), key.size());
    Dbt db_value(NULL, 0);
    db_value.set_flags(DB_DBT_MALLOC);

    try {
	if (db->get(NULL, &db_key, &db_value, DB_READ_UNCOMMITTED) != 0)
	    return false;
    } catch (DbException &e) {
	ERROR("failed to get page from the DB, error is: " << e.what());
	return false;
    }
    *value = buffer_wrapper(static_cast<char *>(db_value.get_data()), db_value.get_size());

    return buffer_wrapper_cache->write(key, *value);
}

bool bdb_bw_map::write(const buffer_wrapper &key, const buffer_wrapper &value) {
    Dbt db_key(key.get(), key.size());
    Dbt db_value(value.get(), value.size());
    try {
	if (db->put(NULL, &db_key, &db_value, 0) != 0)
	    return false;
    } catch (DbException &e) {
	ERROR("failed to put page in the DB, error is: " << e.what());
	return false;
    }
    if (buffer_wrapper_cache->write(key, value)) {
	space_left -= value.size();
	return true;
    } else 
	return false;
}
