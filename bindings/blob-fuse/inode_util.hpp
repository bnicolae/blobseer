#ifndef __INODE
#define __INODE

/*
Inodes are of the form:
|...blob_id...|...version...|
|<---32bit--->|<---32bit--->|
*/

#define ino_id(ino) ((ino) >> 32)
#define ino_version(ino) ((ino) & 0x00000000FFFFFFFF) 
#define build_ino(id, version) (((boost::uint64_t)(id) << 32) + (version))

#endif
