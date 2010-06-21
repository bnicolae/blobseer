#ifndef BLOB_IOCTL
#define BLOB_IOCTL

#include <sys/ioctl.h>

#define COMMIT             _IOR(0, 1, boost::uint64_t)
#define CLONE_AND_COMMIT   _IOR(0, 2, boost::uint64_t)

#endif
