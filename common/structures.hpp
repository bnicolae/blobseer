#ifndef __STRUCTURES
#define __STRUCTURES

#include "provider/provider_adv.hpp"
#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>

#include <ostream>

namespace metadata {

static const unsigned int ROOT = 0, LEFT_CHILD = 1, RIGHT_CHILD = 2;

class provider_desc {
public:
    std::string host, service;

    provider_desc() { }
    provider_desc(const std::string &h, const std::string &s) : host(h), service(s) { }
    
    bool empty() {
	return host == "" && service == "";
    }

    friend std::ostream &operator<<(std::ostream &out, const provider_desc &provider) {
	out << "(" << provider.host << ", " << provider.service << ")";
	return out;
    }

    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & host & service;
    }
};

typedef std::vector<provider_desc> replica_list_t;

class query_t {
public:
    boost::uint32_t id, version;
    boost::uint64_t offset, size;

    friend std::ostream &operator<<(std::ostream &out, const query_t &query) {
	out << "(" << query.id << ", " << query.version << ", " 
	    << query.offset << ", " << query.size << ")";
	return out;
    }

    query_t(boost::uint32_t i, boost::uint32_t v, boost::uint64_t o, boost::uint64_t s) :
	id(i), version(v), offset(o), size(s) { }

    query_t() : id(0), version(0), offset(0), size(0) { }

    bool intersects(const query_t &second) const {
	if (offset <= second.offset)
	    return second.offset < offset + size;
	else
	    return offset < second.offset + second.size;
    }

    bool operator<(const query_t &second) const {
	return version < second.version;
    }

    bool operator==(const query_t &second) const {
	return id == second.id && version == second.version && 
	    offset == second.offset && size == second.size;
    }

    bool empty() const {
	return id == 0 && version == 0 && offset == 0 && size == 0;
    }

    unsigned int getParent(query_t &dest) const {
	dest.id = id;
	dest.version = version;
	dest.size = 2 * size;
	if (offset % dest.size == 0) {
	    dest.offset = offset;
	    return LEFT_CHILD;
	} else {
	    dest.offset = offset - size;
	    return RIGHT_CHILD;
	}
    }
  
    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & id & version & offset & size;
    }
};

class root_t {
public:
    query_t node;
    boost::uint32_t replica_count;
    boost::uint64_t current_size, page_size;    

    root_t() : node(0, 0, 0, 0), replica_count(0), current_size(0), page_size(0) { }

    root_t(boost::uint32_t i, boost::uint32_t v, boost::uint64_t ps, boost::uint64_t ms, boost::uint32_t rc) :
	node(i, v, 0, ms), replica_count(rc), current_size(0), page_size(ps) { }

    bool empty() const {
	return node.id == 0;
    }

    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & node & page_size & current_size & replica_count;
    }
};

class dhtnode_t {
public:
    bool is_leaf;
    query_t left, right;
    boost::uint32_t access_count;
    
    dhtnode_t(bool leaf) : is_leaf(leaf) { }

    friend std::ostream &operator<<(std::ostream &out, const dhtnode_t &node) {
	out << "(left = " << node.left << ", right = " << node.right;
	if (node.is_leaf)
	    out << ", node is leaf";
	out << ")";
	return out;
    }

    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & left & right & is_leaf & access_count;
    }
};

typedef std::vector<metadata::query_t> siblings_enum_t;

}

class obj_info {
public:
    typedef std::pair<metadata::root_t, bool> root_flag_t;
    typedef std::pair<metadata::query_t, root_flag_t> interval_entry_t;
    typedef std::map<metadata::query_t, root_flag_t> interval_list_t;
    
    std::vector<metadata::root_t> roots;
    interval_list_t intervals;
    boost::uint32_t current_ticket;
    boost::uint64_t max_size, progress_size;
    
    obj_info(boost::uint32_t id, boost::uint64_t ps, boost::uint32_t rc) :
	current_ticket(1), max_size(ps), progress_size(0) {
	roots.push_back(metadata::root_t(id, 0, ps, 0, rc));
    }
    obj_info(metadata::root_t &root) :
	current_ticket(1), max_size(root.node.size), progress_size(root.current_size) {
	roots.push_back(root);
    }
};

class vmgr_reply {    
public:
    obj_info::interval_list_t intervals;
    boost::uint64_t root_size;
    metadata::root_t stable_root;

    vmgr_reply() : stable_root(0, 0, 0, 0, 0) { }
    
    static metadata::query_t search_list(metadata::siblings_enum_t &siblings, boost::uint64_t offset, 
					 boost::uint64_t size) {
	for (unsigned int i = 0; i < siblings.size(); i++)
	    if (siblings[i].offset == offset && siblings[i].size == size)
		return siblings[i];
	return metadata::query_t();
    }

    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & intervals & stable_root & root_size;
    }
};

#endif
