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
    static const unsigned int NAME_SIZE = 128, IO_READ = 1, IO_PREFETCH = 2;
    static const unsigned int CHUNK_UNTOUCHED = 1, CHUNK_WAITING = 2, CHUNK_PENDING = 3, 
			      CHUNK_COMPLETE = 4, CHUNK_ERROR = 5;
    static const unsigned int THRESHOLD = 10, MAX_PRIO = 0xFFFFFFFF;
    
    typedef std::pair<unsigned int, boost::uint64_t> read_entry_t;
    typedef std::pair<unsigned int, read_entry_t> io_queue_entry_t;
    typedef std::multimap<unsigned int, read_entry_t, std::greater<unsigned int> > io_queue_t;

    void prefetch_exec();
    void enqueue_chunk(boost::uint64_t index);
    bool wait_for_chunk(boost::uint64_t index);

    std::string local_name;
    int fd;
    boost::uint32_t version;
    char *mapping;

    Object blob;
    boost::uint64_t blob_size, page_size;
    std::vector<boost::uint64_t> chunk_map;
    std::vector<bool> written_map;
    blob::prefetch_list_t prefetch_list;
    io_queue_t io_queue;
    boost::mutex io_queue_lock;
    boost::condition nonempty_cond, io_queue_cond;
    boost::thread prefetch_thread;
};

template <class Object>
local_mirror_t<Object>::local_mirror_t(Object b, boost::uint32_t v) : 
    version(v), blob(b), blob_size(b->get_size(v)), page_size(b->get_page_size()),
    chunk_map(blob_size / page_size, CHUNK_UNTOUCHED), written_map(blob_size / page_size, false),
    prefetch_thread(boost::bind(&local_mirror_t<Object>::prefetch_exec, this)) {

    std::stringstream ss;
    ss << "/tmp/blob-mirror-" << b->get_id() << "-" << version;
    local_name = ss.str();
    
    if ((fd = open(local_name.c_str(), 
		   O_RDWR | O_CREAT | O_NOATIME, 0666)) == -1)
	throw std::runtime_error("could not open temporary file: " + local_name);
    flock lock;
    lock.l_type = F_WRLCK; lock.l_whence = SEEK_SET;
    lock.l_start = 0; lock.l_len = blob_size;
    if (fcntl(fd, F_SETLK, &lock) == -1) {
	close(fd);
	throw std::runtime_error("could not lock temporary file: " + local_name);
    }
    if (lseek(fd, 0, SEEK_END) != (off_t)blob_size) {
	lseek(fd, blob_size - 1, SEEK_SET);
	if (::write(fd, &blob_size, 1) != 1) {
	    close(fd);
	    throw std::runtime_error("could not adjust temporary file: " + local_name);
	}
    }

    std::ifstream idx((local_name + ".idx").c_str(), 
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
	throw std::runtime_error("could not mmap temporary file: " + local_name);
    }
}

template <class Object>
local_mirror_t<Object>::~local_mirror_t() {
    prefetch_thread.interrupt();
    prefetch_thread.join();

    munmap(mapping, blob_size);
    close(fd);

    std::ofstream idx((local_name + ".idx").c_str(), 
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
void local_mirror_t<Object>::prefetch_exec() {
    read_entry_t entry;

    //pthread_setschedprio(prefetch_thread.native_handle(), 90);

    while (1) {
	// Explicit block to specify lock scope
	{
	    boost::mutex::scoped_lock lock(io_queue_lock);
	    while (io_queue.empty())
		nonempty_cond.wait(lock);
	    entry = io_queue.begin()->second;
	    io_queue.erase(io_queue.begin());
	    chunk_map[entry.second / page_size] = CHUNK_PENDING;
	}
	// Explicit block to specify uninterruptible execution scope
	{
	    boost::this_thread::disable_interruption di;
	    bool result = false;

	    switch(entry.first) {
	    case IO_PREFETCH:
		result = blob->read(entry.second, page_size, mapping + entry.second, version);
		DBG("READ OP (PREFETCH) - remote read request (off, size) = (" 
		    << entry.second << ", " << page_size << ")");		    
		{
		    boost::mutex::scoped_lock lock(io_queue_lock);
		    chunk_map[entry.second / page_size] = result ? CHUNK_COMPLETE : CHUNK_ERROR;
		    io_queue_cond.notify_all();
		}
		
		break;

	    case IO_READ:
		result = blob->read(entry.second, page_size, mapping + entry.second, version, THRESHOLD, 
			    	    prefetch_list); 
		DBG("READ OP (DEMAND) - remote read request (off, size) = (" 
		    << entry.second << ", " << page_size << ")");
		{
		    boost::mutex::scoped_lock lock(io_queue_lock);
		    chunk_map[entry.second / page_size] = result ? CHUNK_COMPLETE : CHUNK_ERROR;
		    io_queue_cond.notify_all();
		}

		for (blob::prefetch_list_t::iterator i = prefetch_list.begin(); 
		     i != prefetch_list.end(); i++) {
		    boost::uint64_t index = i->first / page_size;
		    if (chunk_map[index] == CHUNK_UNTOUCHED) {
			boost::mutex::scoped_lock lock(io_queue_lock);
			if (chunk_map[index] == CHUNK_UNTOUCHED) {
			    io_queue.insert(io_queue_entry_t(i->second,
							     read_entry_t(IO_PREFETCH, i->first)));
			    chunk_map[index] = CHUNK_WAITING;
			}
		    }
		}
		
		break;	

	    default:
		DBG("Unknown READ request, ignored");
	    }
	}
    }
}

template <class Object>
void local_mirror_t<Object>::enqueue_chunk(boost::uint64_t index) {
    if (chunk_map[index] != CHUNK_COMPLETE) {
	boost::mutex::scoped_lock lock(io_queue_lock);
	switch (chunk_map[index]) {
	case CHUNK_WAITING:
	    for (io_queue_t::iterator i = io_queue.begin(); i != io_queue.end(); i++)
		if (i->second.second == index * page_size) {
		    io_queue.erase(i);
		    io_queue.insert(io_queue_entry_t(MAX_PRIO, read_entry_t(IO_READ, index * page_size)));
		    break;
		}
	    break;
	case CHUNK_UNTOUCHED:
	    io_queue.insert(io_queue_entry_t(MAX_PRIO, read_entry_t(IO_READ, index * page_size)));
	    chunk_map[index] = CHUNK_WAITING;
	    nonempty_cond.notify_one();
	    break;
	}
    }
}

template <class Object>
bool local_mirror_t<Object>::wait_for_chunk(boost::uint64_t index) {
    if (chunk_map[index] != CHUNK_COMPLETE) {
	boost::mutex::scoped_lock lock(io_queue_lock);

	while (chunk_map[index] != CHUNK_COMPLETE && chunk_map[index] != CHUNK_ERROR)
	    io_queue_cond.wait(lock);
    }

    return chunk_map[index] == CHUNK_COMPLETE;
}

template <class Object>
boost::uint64_t local_mirror_t<Object>::read(size_t size, off_t off, char * &buf) {
    buf = NULL;
    if ((boost::uint64_t)off >= blob_size)
	return 0;
    if ((boost::uint64_t)off + size > blob_size)
	size = blob_size - off;

    boost::uint64_t index;

    for (index = off / page_size; index * page_size < off + size; index++)
	enqueue_chunk(index);

    for (index = off / page_size; index * page_size < off + size; index++)
	if (!wait_for_chunk(index))
	    return 0;

    buf = mapping + off;
    return size;
}

template <class Object>
boost::uint64_t local_mirror_t<Object>::write(size_t size, off_t off, const char *buf) {
    if (off + size > blob_size)
	return 0;

    boost::uint64_t index;

    for (index = off / page_size; index * page_size < off + size; index++)
	enqueue_chunk(index);

    for (index = off / page_size; index * page_size < off + size; index++)
	if (!wait_for_chunk(index))
	    return 0;

    memcpy(mapping + off, buf, size);
    for (index = off / page_size; index * page_size < off + size; index++)
	written_map[index] = true;

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
	    version = blob->write(index * page_size, (stop - index) * page_size, 
				      mapping + index * page_size);
	    if (version == 0)
		return false;
	    for (unsigned int i = index; i < stop; i++) 
		written_map[i] = false;
	    index = stop + 1;
	} else
	    index++;

    std::string old_name = local_name;
    std::stringstream ss;
    ss << "/tmp/blob-fuse-" << blob->get_id() << "-" << version;
    local_name = ss.str();

    if (rename(old_name.c_str(), local_name.c_str()) == -1) {
	local_name = old_name;
	return false;
    }
    unlink((old_name + ".idx").c_str());
    
    return true;
}

template <class Object>
bool local_mirror_t<Object>::clone_and_commit() {
    
    if (!blob->clone())
	return false;
    return commit();
}
    
#endif
