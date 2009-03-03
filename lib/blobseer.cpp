#include <cstdlib>
#include <cassert>
#include <cstring>

#include "blobseer.h"
#include "client/object_handler.hpp"

void __attribute__ ((constructor)) blobtamer_init() {
}
void __attribute__ ((destructor)) blobtamer_fini() {
}

static int count;

extern "C" int blob_init(const char *config_file, blob_env_t *env) {
    env->cfg_file = static_cast<void *>(new std::string(config_file));
    return 1;
}

extern "C" int blob_finalize(blob_env_t *env) {
    delete static_cast<std::string *>(env->cfg_file);
    return 1;
}

extern "C" int blob_create(blob_env_t *env, offset_t page_size, unsigned int replica_count, blob_t *blob) {
    object_handler *h = new object_handler(*(static_cast<std::string *>(env->cfg_file)));
    if (!h->create(page_size, replica_count))
	return 0;
    blob->obj = static_cast<void *>(h);
    blob->page_size = page_size;
    blob->id = h->get_id();
    count = 0;
    return 1;
}

extern "C" int blob_setid(blob_env_t *env, unsigned int id, blob_t *blob) {
    object_handler *h = new object_handler(*(static_cast<std::string *>(env->cfg_file)));
    if (!h->get_latest(id))
	return 0;
    blob->obj = static_cast<void *>(h);
    blob->page_size = h->get_page_size();
    blob->id = id;
    count = 0;
    return 1;
}

extern "C" int blob_free(blob_env_t */*env*/, blob_t *blob) {
    delete static_cast<object_handler *>(blob->obj);
    return 1;
}

extern "C" offset_t blob_getsize(blob_t *blob) {
    object_handler *h = static_cast<object_handler *>(blob->obj);
    offset_t result;

    h->get_latest(blob->id, &result);
    return result;
}

extern "C" int blob_read(blob_t *blob, offset_t offset, offset_t size, char *buffer) {
    bool ret = false;
    
    object_handler *h = static_cast<object_handler *>(blob->obj);
    h->get_latest();
    try {
	if (size < blob->page_size) {
	    char *new_buffer = new char[blob->page_size];
	    ret = h->read(offset, blob->page_size, new_buffer);
	    if (ret)
		memcpy(buffer, new_buffer, size);
	    delete []new_buffer;
	} else
	    ret = h->read(offset, size, buffer);
    } catch (std::exception &e) {
	std::cout << "READ ERROR: " << e.what() << std::endl;
	return 0;
    }
    
    if (!ret) {
	std::cout << "READ ERROR: " << offset << ", " << size << std::endl;
	return 0;
    } else
	return 1;
}

extern "C" int blob_write(blob_t *blob, offset_t offset, offset_t size, char *buffer) {
    object_handler *h = static_cast<object_handler *>(blob->obj);
    // std::cout << "count (write) = " << count++ << std::endl;
    return h->write(offset, size, buffer);
}
