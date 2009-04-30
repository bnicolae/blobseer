#include "blobseer.h"

#include <stdio.h>
#include <string.h>

int main() {
    blob_env_t env;
    blob_t blob;
    char result[1024];

    strcpy(result, "AAAAAAAAAAAAAAAAAAAA");

    printf("Testing C++ library from within C...\n");

    blob_init("test.cfg", &env);
    if (!blob_create(&env, 8, 3, &blob)) {
	printf("Error, cannot alloc!\n");
	return 1;
    }

    printf("Blob size is: %d", blob_getsize(&blob,1));

    blob_write(&blob, 0, 8, "BBBBBBBB");
    blob_write(&blob, 0, 16, "FFFFFFFFCCCCCCCC");
    blob_write(&blob, 16, 8, "DDDDDDDD");
    blob_write(&blob, 16, 16, "EEEEEEEEEEEEEEEE");

    unsigned int v = 1;
    offset_t b_size = blob_getsize(&blob, v);

    printf("Blob size is: %d\n", b_size);
    if (blob_read(&blob, v, 0, b_size, result) != 0)
	printf("ERROR reading version %d\n", v);
    else {
	result[b_size] = 0;
	printf("Result is: %s\n", result);
    }

    blob_free(&env, &blob);
    blob_finalize(&env);

    return 0;
}
