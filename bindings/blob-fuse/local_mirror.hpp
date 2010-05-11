#ifndef __LOCAL_MIRROR_T
#define __LOCAL_MIRROR_T

#include <cstring>
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>

#include <boost/dynamic_bitset.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/dynamic_bitset.hpp>

//#define __DEBUG
#include "common/debug.hpp"

template <class Object>
class local_mirror_t {
public:
    local_mirror_t(Object b, boost::uint32_t v);
    ~local_mirror_t();

    Object get_object();
    int flush();
    boost::uint64_t read(size_t size, off_t off, char * &buf);
    boost::uint64_t write(size_t size, off_t off, const char *buf);
    
private:
    static const unsigned int NAME_SIZE = 128;

    char local_name[NAME_SIZE];
    int fd;
    boost::uint32_t version;
    char *mapping;

    Object blob;
    boost::uint64_t blob_size, page_size;
    boost::dynamic_bitset<> chunk_map;
};

template <class Object>
local_mirror_t<Object>::local_mirror_t(Object b, boost::uint32_t v) : 
    version(v), blob(b), blob_size(b->get_size(v)), page_size(b->get_page_size()),
    chunk_map(blob_size / page_size) {

    sprintf(local_name, "/tmp/blob-fuse-%d-%d", b->get_id(), version);
    
    if ((fd = open(local_name, 
		   O_RDWR | O_CREAT | O_NOATIME, 0666)) == -1)
	throw std::runtime_error("could not open temporary file: " + std::string(local_name));
    flock lock;
    lock.l_type = F_WRLCK; lock.l_whence = SEEK_SET;
    lock.l_start = 0; lock.l_len = blob_size;
    if (fcntl(fd, F_SETLK, &lock) == -1) {
	close(fd);
	throw std::runtime_error("could not lock temporary file: " + std::string(local_name));
    }
    if (lseek(fd, 0, SEEK_END) != (off_t)blob_size) {
	lseek(fd, blob_size - 1, SEEK_SET);
	if (::write(fd, &blob_size, 1) != 1) {
	    close(fd);
	    throw std::runtime_error("could not adjust temporary file: " 
				     + std::string(local_name));
	}
    }

    std::ifstream idx((std::string(local_name) + ".idx").c_str(), 
		      std::ios_base::in | std::ios_base::binary);
    if (idx.good()) {
	boost::archive::binary_iarchive ar(idx);
	ar >> chunk_map;
    }

    mapping = (char *)mmap(NULL, blob_size, PROT_READ | PROT_WRITE, 
			   MAP_SHARED /*| MAP_HUGETLB*/, fd, 0);
    if ((void *)mapping == ((void *)-1)) {
	mapping = NULL;
	close(fd);
	throw std::runtime_error("could not mmap temporary file: " + std::string(local_name));
    }
}

template <class Object>
local_mirror_t<Object>::~local_mirror_t() {
    munmap(mapping, blob_size);
    close(fd);
    std::ofstream idx((std::string(local_name) + ".idx").c_str(), 
		      std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    if (idx.good()) {
	boost::archive::binary_oarchive ar(idx);
	ar << chunk_map;
    }
}

template <class Object>
Object local_mirror_t<Object>::get_object() {
    return blob;
}

template <class Object>
int local_mirror_t<Object>::flush() {
    return msync(mapping, blob_size, MS_ASYNC);
}

template <class Object>
boost::uint64_t local_mirror_t<Object>::read(size_t size, off_t off, char * &buf) {
    buf = NULL;
    if ((boost::uint64_t)off >= blob_size)
	return 0;
    if ((boost::uint64_t)off + size > blob_size)
	size = blob_size - off;

    boost::uint64_t index = off / page_size; 
    while (index * page_size < off + size) {
	boost::uint64_t new_index = index;
	DBG("main read loop, new_index = " << new_index << 
	    ", page size = " << page_size);
	while(new_index * page_size < off + size && 
	      !chunk_map[new_index])
	    new_index++;
	if (new_index > index) {
	    if (!blob->read(index * page_size, 
			    (new_index - index) * page_size, 
			    mapping + index * page_size, version))
		return 0;
	    for (boost::uint64_t i = index; i < new_index; i++)
		chunk_map[i] = 1;
	    index = new_index;
	} else
	    index++;
    }
    buf = mapping + off;
    return size;
}

template <class Object>
boost::uint64_t local_mirror_t<Object>::write(size_t size, off_t off, const char *buf) {
    if (off + size > blob_size)
	return 0;
    memcpy(mapping + off, buf, size);

    boost::uint64_t index = off / page_size; 
    while (index * page_size < off + size) {
	chunk_map[index] = 1;
	index++;
    }
    return size;
}

#endif
