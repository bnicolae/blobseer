#include "fh_map.hpp"

bool fh_map_t::update_inode(boost::uint64_t id, fuse_ino_t new_ino) {
    boost::mutex::scoped_lock lock(fh_table_lock);
    fh_table_by_id &id_index = fh_table.get<tid>();

    fh_table_by_id::iterator i = id_index.find(id);
    if (i == id_index.end()) 
	return false;
    else {
	fh_map_entry_t e = *i;
	e.ino = new_ino;
	id_index.replace(i, e);
	return true;
    }
}

boost::uint64_t fh_map_t::get_instance(fuse_ino_t ino) {
    boost::mutex::scoped_lock lock(fh_table_lock);
    fh_table_by_ino &ino_index = fh_table.get<tino>();

    fh_table_by_ino::iterator i = ino_index.find(ino);
    if (i == ino_index.end())
	return (boost::uint64_t)NULL;
    else {
	fh_map_entry_t e = *i;
	e.count++;
	ino_index.replace(i, e);
	return e.id;
    }
}

bool fh_map_t::release_instance(boost::uint64_t id) {
    boost::mutex::scoped_lock lock(fh_table_lock);
    fh_table_by_id &id_index = fh_table.get<tid>();

    fh_table_by_id::iterator i = id_index.find(id);
    if (i != id_index.end()) {
	fh_map_entry_t e = *i;
	e.count--;
	if (e.count == 0) {
	    id_index.erase(i);
	    return true;
	} else {
	    id_index.replace(i, e);
	    return false;
	}
    } else
	return false;
}

void fh_map_t::add_instance(boost::uint64_t id, fuse_ino_t ino) {
    boost::mutex::scoped_lock lock(fh_table_lock);
    fh_table_by_ino &ino_index = fh_table.get<tino>();
    
    fh_table_by_ino::iterator i = ino_index.find(ino);
    if (i != ino_index.end())
        throw std::runtime_error("fh_map_t::add_instance(): instance already assigned to inode");
    ino_index.insert(fh_map_entry_t(id, ino));
}
