#include <cstdlib>
#include <cassert>
#include <cstring>

#include "blobseer.h"
#include "client/object_handler.hpp"

void __attribute__ ((constructor)) blobseer_init() {
}
void __attribute__ ((destructor)) blobseer_fini() {
}

// subroutine blob_init( config_file, environment )
//      config_file : (input) character*
//      environment : (output) integer*8 
extern "C" int blob_init_(char *config_file, int config_len, int64_t *env) {
    	blob_env_t* c_env = (blob_env_t*)malloc(sizeof(blob_env_t));
    	c_env->cfg_file = static_cast<void *>(new std::string(config_file,config_len));
    	*env = (int64_t)(c_env);
    	return 1;
}

// subroutine blob_finalize( environment )
//	environment : (input/output) integer*8
extern "C" int blob_finalize_(int64_t *env) {
    	delete static_cast<std::string *>(((blob_env_t*)(*env))->cfg_file);
    	free(env);
    	return 1;
}

static int blob_create_c(blob_env_t* env,  offset_t page_size, unsigned int replica_count, blob_t *blob) {
    	object_handler *h = new object_handler(*(static_cast<std::string *>(env->cfg_file)));
    	if (!h->create(page_size, replica_count))
		return 0;

    	blob->obj = static_cast<void *>(h);
    	blob->id = h->get_id();
    	blob->page_size = h->get_page_size();
    	blob->latest_version = h->get_version();

    	return 1;
}

// subroutine blob_create( environment, page_size, replica_count, blob, err )
//	environment : (input) integer*8
//	page_size : (input) integer*8
//	replica_count : (input) integer*8
//	blob : (output) integer*8
//	err : (output) integer*4
extern "C" int blob_create_(int64_t *env, int64_t *page_size, int32_t *replica_count, int64_t* blob, int32_t* err) {
	blob_t* c_blob = (blob_t*)malloc(sizeof(blob_t));
	*blob = (int64_t)c_blob;
	*err = blob_create_c((blob_env_t*)(*env),(offset_t)(*page_size),(unsigned int)(*replica_count),(blob_t*)(*blob));
	if(*err == 0) free(c_blob);
	return 1;
}

static int blob_bind_to_c(blob_env_t *env, unsigned int id, blob_t *blob) {
    	object_handler *h = new object_handler(*(static_cast<std::string *>(env->cfg_file)));
    	if (!h->get_latest(id))
		return 0;

    	blob->obj = static_cast<void *>(h);
    	blob->id = id;
    	blob->page_size = h->get_page_size();
    	blob->latest_version = h->get_version();

    	return 1;
}

// subroutine blob_bind_to( environment, id, blob, err )
// 	environment : (input) integer*8
//	id : (input) integer*8
//	blob : (output) integer*8
//	err : (output) integer*4
extern "C" int blob_bind_to_(int64_t *env, int32_t *id, int64_t* blob, int32_t* err) {
	blob_t* c_blob = (blob_t*)malloc(sizeof(blob_t));
        *blob = (int64_t)c_blob;
	*err = blob_bind_to_c((blob_env_t*)(*env),(unsigned int)(*id),(blob_t*)(*blob));
	if(*err == 0) free(c_blob);
	return 1;
}

static int blob_setid_c(blob_env_t *env, unsigned int id, blob_t *blob) {
        object_handler *h = (object_handler*)(blob->obj);
	if (!h->get_latest(id))
                return 0;

        blob->id = id;
        blob->page_size = h->get_page_size();
        blob->latest_version = h->get_version();

        return 1;
}

// subroutine blob_set_id_( environment, id, blob, err )
//      environment : (input) integer*8
//      id : (input) integer*8
//      blob : (input) integer*8
//      err : (output) integer*4
extern "C" int blob_set_id_(int64_t *env, int32_t *id, int64_t* blob, int32_t* err) {
        *err = blob_setid_c((blob_env_t*)(*env),(unsigned int)(*id),(blob_t*)(*blob));
        return 1;
}


static int blob_free_c(blob_env_t */*env*/, blob_t *blob) {
    	delete static_cast<object_handler *>(blob->obj);
    	free(blob);
	blob = NULL;
    	return 1;
}

// subroutine blob_free( environment, blob )
//	environment : (intput) integer*8
//	blob : (input/output) integer*8
extern "C" int blob_free_(int64_t* env, int64_t* blob) {
	blob_free_c((blob_env_t*)(*env),(blob_t*)(*blob));
	return 1;
}

static offset_t blob_getsize_c(blob_t *blob, id_t version) {
    	object_handler *h = static_cast<object_handler *>(blob->obj);
    	return h->get_size(version);
}

// subroutine blob_get_size( blob, version, size )
//	blob : (input) integer*8
//	version : (input) integer*4
//	size : (output) integer*8
extern "C" int blob_get_size_(int64_t *blob, int32_t *version, int64_t* size) {
	*size = (int64_t)blob_getsize_c((blob_t*)(*blob),(id_t)(*version));
	return 1;
}

static int blob_getlatest_c(blob_t *blob) {
    	object_handler *h = static_cast<object_handler *>(blob->obj);
	
    	if (!h->get_latest())
		return 0;

    	blob->latest_version = h->get_version();
    	return 1;
}

// subroutine blob_get_latest( blob, version, err )
//	blob : (input) integer*8
//	version : (output) integer*4
//	err : (output) integer*4
extern "C" int blob_get_latest_(int64_t* blob, int32_t* version, int32_t* err) {
	*err = blob_getlatest_c((blob_t*)(*blob));
	*version = (int32_t)(((blob_t*)(*blob))->latest_version);
	return 1;
}

static int blob_read_c(blob_t *blob, id_t version, offset_t offset, offset_t size, char *buffer) {
    	bool ret = false;
    	
    	object_handler *h = static_cast<object_handler *>(blob->obj);
    	try {
		ret = h->read(offset, size, buffer, version);
    	} catch (std::exception &e) {
		std::cerr << "READ ERROR: " << e.what() << std::endl;
		return 0;
    	}
   	if (!ret) {
		std::cerr << "READ ERROR: " << offset << ", " << size << std::endl;
		return 0;
	} else
		return 1;
}

// subroutine blob_read( blob, version, offset, size, buffer, err)
//	blob : (input) integer*8
//	version : (input) integer*4
//	offset : (input) integer*8
//	size : (input) integer*8
//	buffer : (output) array
//	err : (output) integer*4
extern "C" int blob_read_(int64_t *blob, int32_t* version, int64_t* offset, int64_t* size, char* buffer, int32_t* err) {
	*err = blob_read_c((blob_t*)(*blob),(id_t)(*version),(offset_t)(*offset),(offset_t)(*size),buffer);
	return 1;
}

static int blob_write_c(blob_t *blob, offset_t offset, offset_t size, char *buffer) {
	object_handler *h = static_cast<object_handler *>(blob->obj);
	return h->write(offset, size, buffer);
}

// subroutine blob_write( blob, offset, size, buffer, err )
//	blob : (input) integer*8
//	offset : (input) integer*8
//	size : (input) integer*8
//	buffer : (input) array
//	err : (output) integer*4
extern "C" int blob_write_(int64_t* blob, int64_t* offset, int64_t* size, char* buffer, int32_t* err) {
	*err = blob_write_c((blob_t*)(*blob),(offset_t)(*offset),(offset_t)(*size),buffer);
	return 1;
}

static int blob_append_c(blob_t *blob, offset_t size, char *buffer) {
	object_handler *h = static_cast<object_handler *>(blob->obj);
	return h->append(size, buffer);
}

// subroutine blob_append( blob, size, buffer, err )
//	blob : (input) integer*8
//	size : (input) integer*8
//	buffer : (input) array
//	err : (output) integer*4
extern "C" int blob_append_(int64_t* blob, int64_t* size, char* buffer, int32_t* err) {
	*err = blob_append_c((blob_t*)(*blob),(offset_t)(*size),buffer);
	return 1;
}

// subroutine blob_get_page_size( blob, psize )
//	blob : (input) integer*8
//	psize : (output) integer*8
extern "C" int blob_get_page_size_(int64_t* blob, int64_t* psize) {
	*psize = (int64_t)(((blob_t*)(*blob))->page_size);
	return 1;
}

// subroutine blob_get_id( blob, id )
//	blob : (input) integer*8
//	id : (output) integer*4
extern "C" int blob_get_id_(int64_t* blob, int32_t* id) {
	*id = (int32_t)(((blob_t*)(*blob))->id);
	return 1;
}

static int blob_clone_c(blob_env_t *env, unsigned int id, id_t version, blob_t *blob) {
    	object_handler *h = new object_handler(*(static_cast<std::string *>(env->cfg_file)));
    	if(!h->clone(id,version))
        	return 0;
    	blob->obj = static_cast<void *>(h);
    	blob->id = h->get_id();
    	blob->page_size = h->get_page_size();
    	blob->latest_version = h->get_version();

    	return 1;
}

// subroutine blob_clone( environment, id, version, blob, err )
//	environment : (input) integer*8
//	id : (input) integer*4
//	version : (input) integer*4
//	blob : (output) integer*8
//	err : (output) integer*4
extern "C" int blob_clone_(int64_t* env, int32_t* id, int32_t* version, int64_t* blob, int32_t* err) {
	blob_t* c_blob = (blob_t*)malloc(sizeof(blob_t));
	*err = blob_clone_c((blob_env_t*)(*env),*id,*version,c_blob);
	if(*err == 0) free(c_blob);
	else *blob = (int64_t)(c_blob);
	return 1;
} 
