#include "null_bw_map.hpp"

#include <cstdlib>

#include "common/debug.hpp"

null_bw_map::null_bw_map(const std::string &/*db_name*/, boost::uint64_t cache_size, boost::uint64_t m, unsigned int /*to*/) :
    buffer_wrapper_cache(new cache_t(cache_size)), space_left(m) { }

null_bw_map::~null_bw_map() {
    delete buffer_wrapper_cache;
}

boost::uint64_t null_bw_map::get_free() {
    return space_left;
}

bool null_bw_map::read(const buffer_wrapper &key, buffer_wrapper *value) {
    return buffer_wrapper_cache->read(key, value);
}

bool null_bw_map::write(const buffer_wrapper &key, const buffer_wrapper &value) {
    if (value.size() > space_left)
	return false;
    if (buffer_wrapper_cache->max_size() == buffer_wrapper_cache->size())
	buffer_wrapper_cache->resize(buffer_wrapper_cache->size() + 1);
    buffer_wrapper_cache->write(key, value);
    space_left -= value.size();
    return true;
}
