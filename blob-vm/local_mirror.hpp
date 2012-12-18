/**
 * Copyright (C) 2008-2012 Bogdan Nicolae <bogdan.nicolae@inria.fr>
 *
 * This file is part of BlobSeer, a scalable distributed big data
 * store. Please see README (root of repository) for more details.
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses>.
 */

#ifndef __LOCAL_MIRROR_T
#define __LOCAL_MIRROR_T

#include <cstring>
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <limits.h>

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
    local_mirror_t(fh_map_t *map_, Object b, boost::uint32_t v, 
		   const std::string &migr_svc, bool mirror_flag,
		   bool push_flag, bool push_all_flag);
    ~local_mirror_t();

    Object get_object();
    int flush();
    int sync();
    boost::uint64_t read(size_t size, boost::uint64_t off, char * &buf);
    boost::uint64_t write(size_t size, boost::uint64_t off, const char *buf);
    int commit();
    int clone();
    int migrate_to(const std::string &desc);
    
private:
    static const unsigned int NAME_SIZE = 128;
    static const unsigned int CHUNK_UNTOUCHED = 1, CHUNK_WAITING = 2, CHUNK_PENDING = 3, 
	CHUNK_COMPLETE = 5, CHUNK_ERROR = 6;
    
    static const unsigned int THRESHOLD = UINT_MAX, 
	READ_PRIO = UINT_MAX, MIGR_PULL_PRIO = READ_PRIO - 1, 
	COMMIT_PRIO = MIGR_PULL_PRIO - 1, MIGR_PUSH_PRIO = COMMIT_PRIO - 1;

    static const unsigned int IO_READ = 1, IO_PREFETCH = 2, IO_COMMIT = 3, IO_PUSH = 4;

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
    bool cancel_chunk(boost::uint64_t index);
    bool wait_for_chunk(boost::uint64_t index);
    bool schedule_chunk(boost::uint64_t offset);
    void cow_chunk(boost::uint64_t index);

    std::string local_name;
    int fd;
    boost::uint32_t version, snapshot_marker;
    char *mapping;

    Object blob;
    fh_map_t *fh_map;
    boost::uint64_t blob_size, page_size;
    blob::prefetch_list_t prefetch_list;
    io_queue_t io_queue;

    std::vector<boost::uint64_t> chunk_state;
    std::vector<typename io_queue_t::iterator> io_queue_ref;
    std::vector<bool> written_map;
    boost::mutex written_map_lock;

    boost::mutex io_queue_lock;
    boost::condition nonempty_cond, io_queue_cond;
    boost::thread async_io_thread;

    boost::thread migration_thread;
    bool migr_flag, writes_mirror_flag, migr_push_flag, migr_push_all_flag;
};

template <class Object>
local_mirror_t<Object>::local_mirror_t(fh_map_t *map_, Object b, 
				       boost::uint32_t v, const std::string &migr_svc,
				       bool mirror_flag, bool push_flag, bool push_all_flag) : 
    version(v), snapshot_marker(v), blob(b), fh_map(map_), blob_size(b->get_size(v)), 
    page_size(b->get_page_size()), chunk_state(blob_size / page_size, CHUNK_UNTOUCHED), 
    io_queue_ref(blob_size / page_size, io_queue.end()), written_map(blob_size / page_size, false),
    async_io_thread(boost::bind(&local_mirror_t<Object>::async_io_exec, this)),
    migration_thread(boost::bind(&migration_wrapper::migr_exec, blob.get(),
				 (migration_wrapper::updater_t)
				 boost::bind(&local_mirror_t<Object>::schedule_chunk, this, _1),
				 (migration_wrapper::updater_t)
				 boost::bind(&local_mirror_t<Object>::cancel_chunk, this, _1),
				 boost::cref(migr_svc))), 
    migr_flag(false), writes_mirror_flag(mirror_flag),
    migr_push_flag(push_flag), migr_push_all_flag(push_all_flag)
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
	ar >> chunk_state >> written_map;
    }

    mapping = (char *)mmap(NULL, blob_size, PROT_READ | PROT_WRITE, 
			   MAP_SHARED /*| MAP_HUGETLB*/, fd, 0);
    if ((void *)mapping == ((void *)-1)) {
	mapping = NULL;
	close(fd);
	throw std::runtime_error("could not mmap temporary file: " + local_name);
    }
    blob->assign_mapping(mapping);
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
	ar << chunk_state << written_map;
    }
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
	    if (entry.op != IO_PUSH)
		for (unsigned int index = entry.offset / page_size; 
		     index * page_size < entry.offset + entry.size; index++) {
		    chunk_state[index] = CHUNK_PENDING;		
		    io_queue_ref[index] = io_queue.end();
		}
	}
	// Explicit block to specify uninterruptible execution scope
	{
	    boost::this_thread::disable_interruption di;
	    bool result = false;
	    unsigned int index;

	    switch(entry.op) {
	    case IO_PREFETCH:
		result = blob->read(entry.offset, page_size, mapping + entry.offset, version); 
		DBG("READ OP (PREFETCH) - remote read request (off, size) = (" 
		    << entry.offset << ", " << page_size << ")");
		{
		    boost::mutex::scoped_lock lock(io_queue_lock);
		    chunk_state[entry.offset / page_size] = result ? CHUNK_COMPLETE : CHUNK_ERROR;
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
		    chunk_state[entry.offset / page_size] = result ? CHUNK_COMPLETE : CHUNK_ERROR;
		    io_queue_cond.notify_all();
		}

		for (blob::prefetch_list_t::iterator i = prefetch_list.begin(); 
		     i != prefetch_list.end(); i++) {
		    index = i->first / page_size;
		    if (chunk_state[index] == CHUNK_UNTOUCHED) {
			boost::mutex::scoped_lock lock(io_queue_lock);
			if (chunk_state[index] == CHUNK_UNTOUCHED) {
			    io_queue.insert(io_queue_entry_t(
						i->second, op_entry_t(IO_PREFETCH,
								      i->first, 0, NULL)));
			    chunk_state[index] = CHUNK_WAITING;		    
			}
		    }
		}
		break;	
	
	    case IO_COMMIT:
		version = blob->write(entry.offset, entry.size, entry.buffer);
		DBG("COMMIT OP - remote write request issued (off, size) = (" << 
		    entry.offset << ", " << entry.size << "), left = " 
		    << snapshot_marker - version);

		if (version == 0)
		    ERROR("COMMIT OP - write request (off, size) = (" << 
			  entry.offset << ", " << entry.size << ") failed!");
		else if (version == snapshot_marker) {
		    fh_map->update_inode((boost::uint64_t)this, 
					 build_ino(blob->get_id(), version));
		    DBG("COMMIT OP - operation completed (blob_id, blob_version) = " << 
			blob->get_id() << ", " << version << ")");
		}
		{
		    boost::mutex::scoped_lock lock(io_queue_lock);
		    for (unsigned int index = entry.offset / page_size; 
			 index * page_size < entry.offset + entry.size; index++)
			chunk_state[index] = CHUNK_COMPLETE;		    
		}
		io_queue_cond.notify_all();

		// clean-up if write triggered cow
		if (entry.buffer != mapping + entry.offset)
		    delete []entry.buffer;

		break;

	    case IO_PUSH:
		DBG("MIGRATE OP - push request (off, size) = (" <<
		    entry.offset << ", " << page_size << ")");
		index = entry.offset / page_size;
		if (!blob->push_chunk(index, mapping + entry.offset))
		    blob->touch(index);
		else if (migr_flag || migr_push_all_flag) {
		    index = blob->next_chunk_to_push();
		    if (index != UINT_MAX) {
			boost::mutex::scoped_lock lock(io_queue_lock);
			io_queue.insert(io_queue_entry_t(MIGR_PUSH_PRIO, 
							 op_entry_t(IO_PUSH, 
								    (boost::uint64_t)index 
								    * page_size, 0, NULL)));
		    }
		}
		break;

	    default:
		FATAL("Invalid background op");
	    }

	}
    }
}

template <class Object>
int local_mirror_t<Object>::flush() {
    return msync(mapping, blob_size, MS_ASYNC);
}

template <class Object>
bool local_mirror_t<Object>::schedule_chunk(boost::uint64_t offset) {
    boost::mutex::scoped_lock lock(io_queue_lock);

    unsigned int index = offset / page_size;
    if (chunk_state[index] != CHUNK_WAITING) {
	io_queue.insert(io_queue_entry_t(MIGR_PULL_PRIO, 
					 op_entry_t(IO_PREFETCH, offset, 0, NULL)));
	chunk_state[index] = CHUNK_WAITING;
	nonempty_cond.notify_one();
	return true;
    } else
	return false;
}

template <class Object>
int local_mirror_t<Object>::migrate_to(const std::string &desc) {
    size_t pos = desc.find(':');
    if (pos >= desc.length() - 1)
	return -1;

    migr_flag = true;
    blob->start(desc.substr(0, pos), desc.substr(pos + 1), written_map);
    if (migr_push_flag) {
	unsigned int index = blob->next_chunk_to_push();
	if (index != UINT_MAX) {
	    boost::mutex::scoped_lock lock(io_queue_lock);
	    io_queue.insert(io_queue_entry_t(MIGR_PUSH_PRIO, 
					     op_entry_t(IO_PUSH, (boost::uint64_t)index 
							* page_size, 0, NULL)));
	    nonempty_cond.notify_one();
	}
    }

    return 0;
}

template <class Object>
int local_mirror_t<Object>::sync() {
    bool during_migration = false;

    if (migr_flag) {
        during_migration = true;
        migr_flag = false;
        DBG("about to transfer control from source to destination");
    } else
        DBG("sync request outside of migration");

    {
        boost::mutex::scoped_lock lock(io_queue_lock);
    
        while (!io_queue.empty())
            io_queue_cond.wait(lock);
    }

    if (during_migration) {
        if (migr_push_all_flag) {
            unsigned int index;
            while (true) {
                index = blob->next_chunk_to_push();
                if (index != UINT_MAX)
                    blob->push_chunk(index, mapping + index * page_size);
                else
                    break;
            }
        }
        blob->transfer_control();
        DBG("control successfully transferred from source to destination");
    }

    return 0;
}

template <class Object>
Object local_mirror_t<Object>::get_object() {
    return blob;
}

template <class Object>
bool local_mirror_t<Object>::cancel_chunk(boost::uint64_t index) {
    if (chunk_state[index] != CHUNK_COMPLETE) {
	boost::mutex::scoped_lock lock(io_queue_lock);

	// wait if CHUNK_PENDING
	if (chunk_state[index] == CHUNK_PENDING)
	    while (chunk_state[index] != CHUNK_COMPLETE && chunk_state[index] != CHUNK_ERROR)
		io_queue_cond.wait(lock);
	// remove from io_queue if CHUNK_WAITING, then mark as completed
	// (both for CHUNK_WAITING and CHUNK_UNTOUCHED)
	else { 
	    if (chunk_state[index] == CHUNK_WAITING)
		for (typename io_queue_t::iterator i = io_queue.begin(); 
		     i != io_queue.end(); i++)
		    if (i->second.offset == index * page_size)
			io_queue.erase(i);
	    chunk_state[index] = CHUNK_COMPLETE;
	}
	return true;
    } else
	return false;
}

template <class Object>
void local_mirror_t<Object>::enqueue_chunk(boost::uint64_t index) {
    if (chunk_state[index] != CHUNK_COMPLETE) {
	boost::mutex::scoped_lock lock(io_queue_lock);
	switch (chunk_state[index]) {
	case CHUNK_WAITING:
	    for (typename io_queue_t::iterator i = io_queue.begin(); i != io_queue.end(); i++)
		if (i->second.offset == index * page_size) {
		    io_queue.erase(i);
		    io_queue.insert(io_queue_entry_t(READ_PRIO, 
						     op_entry_t(IO_READ, index * page_size,
								0, NULL)));
		    break;
		}
	    break;
	case CHUNK_UNTOUCHED:
	    io_queue.insert(io_queue_entry_t(READ_PRIO, op_entry_t(IO_READ, index * page_size,
								   0, NULL)));
	    chunk_state[index] = CHUNK_WAITING;
	    nonempty_cond.notify_one();
	    break;
	}
    }
}

template <class Object>
bool local_mirror_t<Object>::wait_for_chunk(boost::uint64_t index) {
    if (chunk_state[index] != CHUNK_COMPLETE) {
	boost::mutex::scoped_lock lock(io_queue_lock);
	
	while (chunk_state[index] != CHUNK_COMPLETE && chunk_state[index] != CHUNK_ERROR)
	    io_queue_cond.wait(lock);
    }

    return chunk_state[index] == CHUNK_COMPLETE;
}

template <class Object>
void local_mirror_t<Object>::cow_chunk(boost::uint64_t index) {
    boost::mutex::scoped_lock lock(io_queue_lock);
    typename io_queue_t::iterator it = io_queue_ref[index];
    if (it != io_queue.end()) {
	char *buffer = new char[it->second.size];
	memcpy(buffer, it->second.buffer, it->second.size);
	it->second.buffer = buffer;
	for (unsigned int i = index; i * page_size < it->second.offset + it->second.size; i++)
	    io_queue_ref[i] = io_queue.end();
	DBG("cow initiated for region (off, size) = (" 
	    << it->second.offset << ", " << it->second.size << ")");
    }
}

template <class Object>
boost::uint64_t local_mirror_t<Object>::read(size_t size, boost::uint64_t off, char * &buf) {
    buf = NULL;
    if (off >= blob_size)
	return 0;
    if (off + size > blob_size)
	size = blob_size - off;

    unsigned int index;

    for (index = off / page_size; index * page_size < off + size; index++)
	enqueue_chunk(index);

    for (index = off / page_size; index * page_size < off + size; index++)
	if (!wait_for_chunk(index))
	    return 0;

    buf = mapping + off;
    return size;
}

template <class Object>
boost::uint64_t local_mirror_t<Object>::write(size_t size, boost::uint64_t off, const char *buf) {
    if (off + size > blob_size)
	return 0;

    boost::uint64_t left_part = (page_size - (off % page_size)) % page_size;
    boost::uint64_t right_part = (off + size) % page_size;

    // enqueue chunks at the extremes and cancel background read of fully overwritten chunks
    if (left_part > 0)
	enqueue_chunk(off / page_size);
    else 
	cancel_chunk(off / page_size);
    if (right_part > 0 && off / page_size != (off + size) / page_size)
	enqueue_chunk((off + size) / page_size);
    else
	cancel_chunk((off + size) / page_size);
    for (unsigned int index = off / size + 1; index * page_size < off + size - right_part; index++)
	cancel_chunk(index);

    // wait for chunks at the extremes
    if (left_part > 0 && !wait_for_chunk(off / page_size))
	return 0;
    if (right_part > 0 && off / page_size != (off + size) / page_size 
	&& !wait_for_chunk((off + size) / page_size))
	return 0;

    for (unsigned int index = off / page_size; index * page_size < off + size; index++)
	cow_chunk(index);

    {
	boost::mutex::scoped_lock lock(written_map_lock);

	memcpy(mapping + off, buf, size);
	for (unsigned int index = off / page_size; index * page_size < off + size; index++)
	    written_map[index] = true;
    }
    if (migr_flag) {
	for (unsigned int index = off / page_size; index * page_size < off + size; index++)
	    if (writes_mirror_flag && blob->push_chunk(index, mapping + index * page_size))
		blob->untouch(index);
	    else
		blob->touch(index);
    }
    
    return size;
}

template <class Object>
int local_mirror_t<Object>::commit() {
    unsigned int index = 0, new_version = 0;

    // wait for previous checkpoint to complete
    if (snapshot_marker > version) {
	boost::mutex::scoped_lock lock(io_queue_lock);
	while (snapshot_marker > version)
	    io_queue_cond.wait(lock);
    }

    {
	boost::mutex::scoped_lock lock(written_map_lock);
	
	while (index * page_size < blob_size)
	    if (written_map[index]) {
		unsigned int stop = index + 1;
		while (written_map[stop] && stop * page_size < blob_size)
		    stop++;
		
		{
		    boost::mutex::scoped_lock lock(io_queue_lock);
		    typename io_queue_t::iterator it = io_queue.insert(
			io_queue_entry_t(COMMIT_PRIO, 
					 op_entry_t(IO_COMMIT, 
						    index * page_size,
						    (stop - index) * page_size,
						    mapping + index * page_size)));
		    for (unsigned int i = index; i < stop; i++)
			io_queue_ref[i] = it;
		}
		
		for (unsigned int i = index; i < stop; i++) 
		    written_map[i] = false;
		index = stop + 1;
		new_version++;
	    } else
		index++;
    }
	
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
