#ifndef __BLOBSEER
#define __BLOBSEER

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef uint64_t offset_t;
typedef uint32_t id_t;

typedef struct {   
    void *obj;
    id_t id, latest_version;
    offset_t page_size;    
} blob_t;

typedef struct blob_env {
    void *cfg_file;
} blob_env_t;

int blob_init(const char *config_file, blob_env_t *env);
int blob_finalize(blob_env_t *env);

int blob_create(blob_env_t *env, offset_t page_size, unsigned int replica_count, blob_t *blob);
int blob_setid(blob_env_t *env, unsigned int id, blob_t *blob);
int blob_free(blob_env_t *env, blob_t *blob);


int blob_getlatest(blob_t *blob);
offset_t blob_getsize(blob_t *blob, id_t version);
int blob_read(blob_t *blob, id_t version, offset_t offset, offset_t size, char *buffer);
int blob_write(blob_t *blob, offset_t offset, offset_t size, char *buffer);
int blob_append(blob_t *blob, offset_t size, char *buffer);

#ifdef __cplusplus
}
#endif

#endif
