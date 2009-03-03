#include "blobseer.h"

#include <stdio.h>
#include <string.h>

int main() {
    blob_env_t env;
    blob_t blob;
    char result[21];

    strcpy(result, "AAAAAAAAAAAAAAAAAAAA");

    printf("Testing C++ library from within C...\n");

    blob_init("test.cfg", &env);
    if (!blob_create(&env, 8, 3, &blob)) {
	printf("Error, cannot alloc!\n");
	return 1;
    }
    blob_write(&blob, 0, 8, "BBBBBBBB");
    blob_write(&blob, 8, 8, "CCCCCCCC");
    blob_write(&blob, 16, 8, "DDDDDDDD");
    blob_write(&blob, 16, 16, "EEEEEEEEEEEEEEEE");
    
    if (blob_read(&blob, 8, 1024, result) != 0)
	printf("blob_read does NOT work CORERCTLY: it can read");
    else
	printf("blob_read works CORRECTLY: it cannot read");

    printf("Result is: %s\n", result);

    blob_free(&env, &blob);
    blob_finalize(&env);

    return 0;
}
