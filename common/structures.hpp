#ifndef __STRUCTURES
#define __STRUCTURES

#include "provider/provider_adv.hpp"
#include <boost/serialization/vector.hpp>
#include <ostream>

namespace metadata {

static const unsigned int ROOT = 0, LEFT_CHILD = 1, RIGHT_CHILD = 2;

// first = leftover, second = intersection
// typedef std::pair<std::vector<query_t>, std::vector<query_t> > intersection_t;

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
/*
    intersection_t intersection(const query_t &second) const {
	boost::uint64_t first_left = offset, first_right = offset + size - 1;
	boost::uint64_t second_left = second.offset, second_right = second.offset + second.size - 1;

	intersection_t result;

	if (first_left <= second_left) {
	    if (first_right < second_left)
		return result;
	    if (first_left < second_left) 
		result.first.push_back(query_t(id, version, first_left, second_left - first_left + 1));
	    if (first_right > second_right)
		result.second.push_back(query_t(second.id, second.version, second_left, second_right - second_left + 1));
	    else if () {
	    }		
	}


    }
*/
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

    root_t(boost::uint32_t i, boost::uint32_t v, boost::uint64_t ps, boost::uint64_t ms, boost::uint32_t rc) :
	node(i, v, 0, ms), replica_count(rc), current_size(0), page_size(ps) { }

    const query_t &get_node() const {
	return node;
    }

    boost::uint64_t get_page_size() const {
	return page_size;
    }

    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & node & page_size & current_size & replica_count;
    }
};

class dhtnode_t {
public:
    typedef std::vector<provider_adv> leaf_t;
    query_t left, right;
    leaf_t leaf;

    friend std::ostream &operator<<(std::ostream &out, const dhtnode_t &node) {
	out << "(left = " << node.left << ", right = " << node.right;
	if (!node.leaf.empty()) {
	    out << ", leaf = [";
	    for (unsigned int i = 0; i < node.leaf.size(); i++)
		out << node.leaf[i] << " ";
	    out << "]";
	}
	out << ")";
	return out;
    }

    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & left & right & leaf;
    }
};

}

class vmgr_reply {    
public:
    typedef std::vector<metadata::query_t> siblings_enum_t;
    siblings_enum_t left, right;
    boost::uint32_t ticket;
    boost::uint64_t root_size, append_offset;
    metadata::root_t stable_root;

    vmgr_reply() : stable_root(0, 0, 0, 0, 0) { }
    
    static metadata::query_t search_list(siblings_enum_t &siblings, boost::uint64_t offset, boost::uint64_t size) {
	for (unsigned int i = 0; i < siblings.size(); i++)
	    if (siblings[i].offset == offset && siblings[i].size == size)
		return siblings[i];
	return metadata::query_t();
    }

    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & stable_root & left & right & root_size & ticket & append_offset;
    }
};

#endif
