#ifndef __LOCAL_MIRROR_T
#define __LOCAL_MIRROR_T

#include <cstring>
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include "inode_util.hpp"
#include "fh_map.hpp"

#define __DEBUG
#include "common/debug.hpp"

#define min(x, y) ((x) < (y) ? (x) : (y))

template <class Object>
class local_mirror_t {
public:
    local_mirror_t(fh_map_t *map_, Object b, boost::uint32_t v, const std::string &migr_svc);
    ~local_mirror_t();

    Object get_object();
    int flush();
    int sync();
    boost::uint64_t read(size_t size, off_t off, char * &buf);
    boost::uint64_t write(size_t size, off_t off, const char *buf);
    int commit();
    int clone();
    int migrate_to(const std::string &host, const std::string &port);
    
private:
    static const unsigned int NAME_SIZE = 128, IO_READ = 1, IO_PREFETCH = 2, IO_COMMIT = 3;
    static const unsigned int CHUNK_UNTOUCHED = 1, CHUNK_WAITING = 2, CHUNK_PENDING = 3, 
			      CHUNK_COMPLETE = 4, CHUNK_ERROR = 5;
    static const unsigned int THRESHOLD = 0xFFFFFFFF, MAX_PRIO = 0xFFFFFFFF;

    struct op_entry_t {
	unsigned int op;
	boost::uint64_t offset, size;
	char *buffer;
	
	op_entry_t(): op(0), offset(0), size(0), buffer(NULL) { }
	op_entry_t(unsigned int op_, boost::uint64_t offset_, 
		   boost::uint64_t size_, char *buffer_):
	op(op_), offset(offset_), size(size_), buffer(buffer_) { }
    };
    
    typedef std::pair<unsigned int, op_entry_t> io_queue_entry_t;
    typedef std::multimap<unsigned int, op_entry_t, std::greater<unsigned int> > io_queue_t;

    void async_io_exec();
    void enqueue_chunk(boost::uint64_t index);
    bool wait_for_chunk(boost::uint64_t index);
    void schedule_chunk(boost::uint64_t offset);

    std::string local_name;
    int fd;
    boost::uint32_t version, snapshot_marker;
    char *mapping;

    Object blob;
    fh_map_t *fh_map;
    boost::uint64_t blob_size, page_size;
    std::vector<boost::uint64_t> chunk_map;
    std::vector<bool> written_map;
    boost::mutex written_map_lock;

    blob::prefetch_list_t prefetch_list;
    io_queue_t io_queue;
    boost::mutex io_queue_lock;
    boost::condition nonempty_cond, io_queue_cond;
    boost::thread async_io_thread;

    boost::thread migration_thread;
    bool migr_flag;
    std::string migr_host, migr_port; 
};

template <class Object>
local_mirror_t<Object>::local_mirror_t(fh_map_t *map_, Object b, 
				       boost::uint32_t v, const std::string &migr_svc) : 
    version(v), snapshot_marker(v), blob(b), fh_map(map_), blob_size(b->get_size(v)), 
    page_size(b->get_page_size()), chunk_map(blob_size / page_size, CHUNK_UNTOUCHED), 
    written_map(blob_size / page_size, false),
    async_io_thread(boost::bind(&local_mirror_t<Object>::async_io_exec, this)),
    migration_thread(boost::bind(&migration_wrapper::migr_exec, blob.get(),
				 (migration_wrapper::updater_t)
				 boost::bind(&local_mirror_t<Object>::schedule_chunk, this, _1),
				 boost::cref(migr_svc))), migr_flag(false)
{

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
    DBG("destructor for (blob_id, blob_version) = (" 	    
	<< blob->get_id() << ", " << version << ")");
    sync();

    async_io_thread.interrupt();
    async_io_thread.join();
    blob->terminate();
    migration_thread.join();

    munmap(mapping, blob_size);
    close(fd);

    std::string old_name = local_name;
    std::stringstream ss;
    ss << "/tmp/blob-mirror-" << blob->get_id() << "-" << version;
    local_name = ss.str();

    if (local_name != old_name && rename(old_name.c_str(), local_name.c_str()) == -1)
	local_name = old_name;

    std::ofstream idx((local_name + ".idx").c_str(), 
		      std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    if (idx.good()) {
	boost::archive::binary_oarchive ar(idx);
	ar << chunk_map << written_map;
    }
}

template <class Object>
int local_mirror_t<Object>::flush() {
    return msync(mapping, blob_size, MS_ASYNC);
}

template <class Object>
void local_mirror_t<Object>::schedule_chunk(boost::uint64_t offset) {
    boost::mutex::scoped_lock lock(io_queue_lock);
    io_queue.insert(io_queue_entry_t(MAX_PRIO - 1, op_entry_t(IO_PREFETCH, offset, 0, NULL)));
}

template <class Object>
int local_mirror_t<Object>::migrate_to(const std::string &host, const std::string &port) {
    migr_flag = true;
    migr_host = host;
    migr_port = port;

    return 0;
}

template <class Object>
int local_mirror_t<Object>::sync() {
    if (migr_flag) {
	blob->start(migr_host, migr_port, mapping, written_map);
	migr_flag = false;
    }

    {
	boost::mutex::scoped_lock lock(io_queue_lock);
    
	while (!io_queue.empty())
	    io_queue_cond.wait(lock);
    }

    return 0;
}

template <class Object>
Object local_mirror_t<Object>::get_object() {
    return blob;
}

template <class Object>
void local_mirror_t<Object>::async_io_exec() {
    op_entry_t entry;

    //pthread_setschedprio(prefetch_thread.native_handle(), 90);

    while (1) {
	// Explicit block to specify lock scope
	{
	    boost::mutex::scoped_lock lock(io_queue_lock);
	    while (io_queue.empty())
		nonempty_cond.wait(lock);
	    entry = io_queue.begin()->second;
	    io_queue.erase(io_queue.begin());
	    if (chunk_map[entry.offset / page_size] != CHUNK_COMPLETE)
		chunk_map[entry.offset / page_size] = CHUNK_PENDING;
	}
	// Explicit block to specify uninterruptible execution scope
	{
	    boost::this_thread::disable_interruption di;
	    bool result = false;

	    switch(entry.op) {
	    case IO_PREFETCH:
		result = blob->read(entry.offset, page_size, mapping + entry.offset, version); 
		DBG("READ OP (PREFETCH) - remote read request (off, size) = (" 
		    << entry.offset << ", " << page_size << ")");
		{
		    boost::mutex::scoped_lock lock(io_queue_lock);
		    chunk_map[entry.offset / page_size] = result ? CHUNK_COMPLETE : CHUNK_ERROR;
		    io_queue_cond.notify_all();
		}		
		break;

	    case IO_READ:
		result = blob->read(entry.offset, page_size, mapping + entry.offset, 
				    version, THRESHOLD, prefetch_list); 
		DBG("READ OP (DEMAND) - remote read request (off, size) = (" 
		    << entry.offset << ", " << page_size << ")");
		{
		    boost::mutex::scoped_lock lock(io_queue_lock);
		    chunk_map[entry.offset / page_size] = result ? CHUNK_COMPLETE : CHUNK_ERROR;
		    io_queue_cond.notify_all();
		}

		for (blob::prefetch_list_t::iterator i = prefetch_list.begin(); 
		     i != prefetch_list.end(); i++) {
		    unsigned int index = i->first / page_size;
		    if (chunk_map[index] == CHUNK_UNTOUCHED) {
			boost::mutex::scoped_lock lock(io_queue_lock);
			if (chunk_map[index] == CHUNK_UNTOUCHED) {
			    io_queue.insert(io_queue_entry_t(i->second,
							     op_entry_t(IO_PREFETCH,
									i->first, 0, NULL)));
			    chunk_map[index] = CHUNK_WAITING;
			}
		    }
		}
		break;	
	
	    case IO_COMMIT:
		DBG("COMMIT OP - remote write request issued (off, size) = (" << 
		    entry.offset << ", " << entry.size << "), left = " 
		    << snapshot_marker - version);
		version = blob->write(entry.offset, entry.size, entry.buffer);
		if (version == 0)
		    ERROR("COMMIT OP - write request (off, size) = (" << 
			  entry.offset << ", " << entry.size << ") failed!");
		if (version == snapshot_marker) {
		    fh_map->update_inode((boost::uint64_t)this, 
					 build_ino(blob->get_id(), version));
		    DBG("COMMIT OP - operation completed (blob_id, blob_version) = " << 
			blob->get_id() << ", " << version << ")");
		}
		delete []entry.buffer;
		io_queue_cond.notify_all();

		break;

	    default:
		FATAL("Invalid background op");
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
	    for (typename io_queue_t::iterator i = io_queue.begin(); i != io_queue.end(); i++)
		if (i->second.offset == index * page_size) {
		    io_queue.erase(i);
		    io_queue.insert(io_queue_entry_t(MAX_PRIO, 
						     op_entry_t(IO_READ, index * page_size,
								0, NULL)));
		    break;
		}
	    break;
	case CHUNK_UNTOUCHED:
	    io_queue.insert(io_queue_entry_t(MAX_PRIO, op_entry_t(IO_READ, index * page_size,
								  0, NULL)));
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

    boost::uint64_t left_part = (page_size - (off % page_size)) % page_size;
    boost::uint64_t right_part = (off + size) % page_size;

    // enqueue chunks at the extremes
    if (left_part > 0)
	enqueue_chunk(off / page_size);
    if (right_part > 0 && off / page_size != (off + size) / page_size)
	enqueue_chunk((off + size) / page_size);

    // wait for chunks at the extremes
    if (left_part > 0 && !wait_for_chunk(off / page_size))
	return 0;
    if (right_part > 0 && off / page_size != (off + size) / page_size 
	&& !wait_for_chunk((off + size) / page_size))
	return 0;

    {
	boost::mutex::scoped_lock lock(written_map_lock);

	memcpy(mapping + off, buf, size);
	for (boost::uint64_t index = off / page_size; index * page_size < off + size; index++)
	    written_map[index] = true;
    }

    return size;
}

template <class Object>
int local_mirror_t<Object>::commit() {
    unsigned int index = 0, new_version = 0;
    boost::mutex::scoped_lock lock(written_map_lock);

    while (index * page_size < blob_size)
	if (written_map[index]) {
	    unsigned int stop = index + 1;
	    while (written_map[stop] && stop * page_size < blob_size)
		stop++;

	    char *region = new char[(stop - index) * page_size];
	    memcpy(region, mapping + index * page_size, (stop - index) * page_size);
	    {
		boost::mutex::scoped_lock lock(io_queue_lock);
		io_queue.insert(io_queue_entry_t(MAX_PRIO - 2, 
						 op_entry_t(IO_COMMIT, 
							    index * page_size,
							    (stop - index) * page_size,
							    region)));
	    }

	    for (unsigned int i = index; i < stop; i++) 
		written_map[i] = false;
	    index = stop + 1;
	    new_version++;
	} else
	    index++;

    if (snapshot_marker + new_version > version) {
	snapshot_marker += new_version;
	DBG("COMMIT OP - asynchronous transfer started, snapshot marker = " << snapshot_marker);
	nonempty_cond.notify_one();
    }

    return snapshot_marker;
}

template <class Object>
int local_mirror_t<Object>::clone() {
    sync();
    if (!blob->clone())
	return -1;
    else {
	version = snapshot_marker = 0;
	return blob->get_id();
    }
}
    
#endif
