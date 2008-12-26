#ifndef __BLOB_SEER
#define __BLOB_SEER

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef uint64_t offset_t;

typedef struct {   
    void *obj;
    offset_t page_size;
} blob_t;

typedef struct blob_env {
    void *cfg_file;
} blob_env_t;

int blob_init(const char *config_file, blob_env_t *env);
int blob_finalize(blob_env_t *env);

int blob_create(blob_env_t *env, offset_t page_size, unsigned int replica_count, blob_t *blob);
int blob_free(blob_env_t *env, blob_t *blob);

int blob_read(blob_t *blob, offset_t offset, offset_t size, char *buffer);
int blob_write(blob_t *blob, offset_t offset, offset_t size, char *buffer);

#ifdef __cplusplus
}
#endif

#endif
