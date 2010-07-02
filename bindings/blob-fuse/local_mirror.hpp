#ifndef __LOCAL_MIRROR_T
#define __LOCAL_MIRROR_T

#include <cstring>
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#define __DEBUG
#include "common/debug.hpp"

#define min(x, y) ((x) < (y) ? (x) : (y))

template <class Object>
class local_mirror_t {
public:
    local_mirror_t(Object b, boost::uint32_t v);
    ~local_mirror_t();

    Object get_object();
    int flush();
    boost::uint64_t read(size_t size, off_t off, char * &buf);
    boost::uint64_t write(size_t size, off_t off, const char *buf);
    bool commit();
    bool clone_and_commit();
    
private:
    static const unsigned int NAME_SIZE = 128;

    char local_name[NAME_SIZE];
    int fd;
    boost::uint32_t version;
    char *mapping;

    Object blob;
    boost::uint64_t blob_size, page_size;
    std::vector<boost::uint64_t> chunk_map;
    std::vector<bool> written_map;
};

template <class Object>
local_mirror_t<Object>::local_mirror_t(Object b, boost::uint32_t v) : 
    version(v), blob(b), blob_size(b->get_size(v)), page_size(b->get_page_size()),
    chunk_map(blob_size / page_size), written_map(blob_size / page_size) {

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
	ar >> chunk_map >> written_map;
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
	ar << chunk_map << written_map;
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
	if (chunk_map[index] < min(page_size, off + size - index * page_size)) {
	    boost::uint64_t read_off = index * page_size, read_size = 0; 
	    if (chunk_map[index] > 0) {
		read_off += chunk_map[index];
		read_size += page_size - chunk_map[index];
	    }
	    while (read_off + read_size < off + size &&
		   chunk_map[(read_off + read_size) / page_size] == 0)
		read_size += page_size;
	    DBG("READ OP - remote read request issued (off, size) = (" << read_off << 
		", " << read_size << ")");
	    if (!blob->read(read_off, read_size, mapping + read_off, version))
		return 0;
	    while (index * page_size < read_off + read_size) {
		chunk_map[index] = page_size;
		index++;
	    }
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

    boost::uint64_t index = off / page_size,
	gap_off = index * page_size + chunk_map[index];
    if (gap_off < (boost::uint64_t)off) {
	DBG("WRITE OP - remote read request issued (off, size) = (" << gap_off << 
	    ", " << off - gap_off << ")");
	if (!blob->read(gap_off, off - gap_off, mapping + gap_off, version))
	    return 0;
    }
    while (index * page_size < off + size) {
	gap_off = min(page_size, off + size - index * page_size);
	if (chunk_map[index] < gap_off)
	    chunk_map[index] = gap_off;
	written_map[index] = true;
	index++;
    }
    return size;
}

template <class Object>
bool local_mirror_t<Object>::commit() {
    unsigned int index = 0;

    while (index * page_size < blob_size)
	if (written_map[index]) {
	    unsigned int stop = index + 1;
	    while (written_map[stop] && stop * page_size < blob_size)
		stop++;
	    DBG("COMMIT OP - remote write request issued (off, size) = (" << 
		index * page_size << ", " << (stop - index) * page_size << ")");
	    if (!blob->write(index * page_size, (stop - index) * page_size, 
			     mapping + index * page_size))
		return false;
	    for (unsigned int i = index; i < stop; i++) 
		written_map[i] = false;
	    index = stop + 1;
	} else
	    index++;
    
    return true;
}

template <class Object>
bool local_mirror_t<Object>::clone_and_commit() {
    unsigned int old_id = blob->get_id();
    char old_local_name[NAME_SIZE];
    strcpy(old_local_name, local_name);
    
    if (!blob->clone())
	return false;
    sprintf(local_name, "/tmp/blob-fuse-%d-%d", blob->get_id(), blob->get_version());
    if (rename(old_local_name, local_name) == -1) {
	blob->get_latest(old_id);
	return false;
    }
    unlink((std::string(old_local_name) + ".idx").c_str());
    return commit();
}
    
#endif
