#ifndef BLOB_IOCTL
#define BLOB_IOCTL

#include <sys/ioctl.h>
                             
#define CLONE              _IO('B', 0)
#define COMMIT             _IO('B', 1)
#define MIGRATE            _IOC(_IOC_WRITE, 'B', 2, 64)

#endif
