#include "bdb_bw_map.hpp"

#include <cstdlib>

#include "common/debug.hpp"

void *buffer_wrapper_alloc(size_t size) {
    return static_cast<void *>(new char[size]);
}

void buffer_wrapper_free(void *ptr) {
    delete [](static_cast<char *>(ptr));
}

bdb_bw_map::bdb_bw_map(const std::string &db_name, boost::uint64_t cache_size, 
                       boost::uint64_t m) :
    buffer_wrapper_cache(cache_size, boost::bind(&bdb_bw_map::evict, this, _1, _2)), 
    db_env(0), space_left(m),
    process_writes(boost::bind(&bdb_bw_map::write_exec, this))
{
    boost::filesystem::path path(db_name.c_str());
    boost::filesystem::create_directories(path.parent_path());

    DBG("db_name = " << path.filename() << ", path = " << path.parent_path().string().c_str());
    db_env.set_alloc(buffer_wrapper_alloc, realloc, buffer_wrapper_free);
    db_env.open(path.parent_path().string().c_str(),
        DB_INIT_CDB | DB_INIT_MPOOL | DB_THREAD | DB_CREATE, 0);
    db = new Db(&db_env, 0);
    db->set_error_stream(&std::cerr);
    db->open(NULL, path.filename().c_str(), NULL, DB_HASH, DB_CREATE | DB_THREAD | DB_READ_UNCOMMITTED, 0);
}

void bdb_bw_map::write_exec() {
    write_entry_t entry;

    while (1) {
	// Explicit block to specify lock scope
	{
	    boost::mutex::scoped_lock lock(write_queue_lock);
	    if (write_queue.empty())
		write_queue_cond.wait(lock);
	    entry = write_queue.front();
	    write_queue.pop_front();
	    write_queue_cond.notify_one();
	}
	// Explicit block to specify uninterruptible execution scope
	{
	    boost::this_thread::disable_interruption di;

	    Dbt db_key(entry.first.get(), entry.first.size());
	    Dbt db_value(entry.second.get(), entry.second.size());
	    
	    try {
		db->put(NULL, &db_key, &db_value, 0);
	    } catch (DbException &e) {
		ERROR("failed to put page in the DB, error is: " << e.what());
	    }
	}
    }
}

bdb_bw_map::~bdb_bw_map() {
    process_writes.interrupt();
    process_writes.join();
    db->close(0);
    delete db;
    db_env.close(0);
}

boost::uint64_t bdb_bw_map::get_free() {
    return space_left;
}

bool bdb_bw_map::find(const buffer_wrapper &key) {
    if (buffer_wrapper_cache.find(key))
	return true;
    Dbt db_key(key.get(), key.size());
    return db->exists(NULL, &db_key, DB_READ_UNCOMMITTED) == 0;
}

bool bdb_bw_map::read(const buffer_wrapper &key, buffer_wrapper *value) {
    if (buffer_wrapper_cache.read(key, value))
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

    return buffer_wrapper_cache.write(key, *value, false);
}

void bdb_bw_map::evict(const buffer_wrapper &key, const buffer_wrapper &value) {
    boost::mutex::scoped_lock lock(write_queue_lock);    
    if (write_queue.size() > MAX_WRITE_QUEUE)
	write_queue_cond.wait(lock);
    write_queue.push_back(write_entry_t(key, value));
    write_queue_cond.notify_one();
}

bool bdb_bw_map::write(const buffer_wrapper &key, const buffer_wrapper &value) {
    if (value.size() > space_left)
	return false;
    bool result = buffer_wrapper_cache.write(key, value);
    if (result)
	space_left -= value.size();
    return result;
}
