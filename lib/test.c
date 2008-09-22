#include "blobtamer.h"

#include <stdio.h>

int main() {
    blob_env_t env;
    blob_t blob;
    char result[20] = "AAAAAAAAAAAAAAAAAAAA";

    printf("Testing C++ library from within C...\n");

    blob_init("test.cfg", &env);
    if (!blob_create(&env, 8, &blob)) {
	printf("Error, cannot alloc!\n");
	return 1;
    }
    blob_write(&blob, 0, 8, "BBBBBBBB");
    cout << "-- written (0, 8)" << endl;
    blob_write(&blob, 8, 8, "CCCCCCCC");
    cout << "-- written (8, 8)" << endl;
    blob_write(&blob, 16, 8, "DDDDDDDD");
    cout << "-- written (16 ,8)" << endl;
    blob_write(&blob, 16, 16, "EEEEEEEEEEEEEEEE");
    cout << "-- written (16 ,16)" << endl;
    blob_read(&blob, 8, 16, result);
    cout << "-- read (8 ,16)" << endl;

    printf("Result is: %s\n", result);

    blob_free(&env, &blob);
    blob_finalize(&env);

    return 0;
}
