#ifndef __VMANAGEMENT
#define __VMANAGEMENT

#include <map>

#include "common/config.hpp"
#include "common/null_lock.hpp"
#include "common/structures.hpp"
#include "rpc/rpc_meta.hpp"

class vmanagement {
public:
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
    };

    rpcreturn_t get_root(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t get_ticket(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t get_objcount(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t create(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);
    rpcreturn_t publish(const rpcvector_t &params, rpcvector_t &result, const std::string &sender);

    ~vmanagement();
    vmanagement();
private:
    typedef boost::unordered_map<unsigned int, obj_info, boost::hash<unsigned int> > obj_hash_t;

    void compute_sibling_versions(vmgr_reply::siblings_enum_t &siblings,
				  metadata::query_t &edge_node,
				  obj_info::interval_list_t &intervals, 
				  boost::uint64_t root_size);
    obj_hash_t obj_hash;
    boost::int32_t obj_count;

    // all operations are serialized and performed in the same thread.
    typedef null_lock::scoped_lock scoped_lock;
    null_lock mgr_lock;
};

#endif
