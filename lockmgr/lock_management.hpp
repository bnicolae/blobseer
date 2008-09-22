#ifndef __LOCK_MANAGEMENT
#define __LOCK_MANAGEMENT

#include <ext/hash_map>
#include <map>

#include "common/config.hpp"
#include "common/structures.hpp"
#include "rpc/rpc_meta.hpp"

class lock_management {
public:
    class obj_info {
    public:
	typedef std::pair<metadata::root_t, bool> root_flag_t;
	typedef std::pair<metadata::query_t, root_flag_t> interval_entry_t;
	typedef std::map<metadata::query_t, root_flag_t> interval_list_t;

	metadata::root_t last_root;
	interval_list_t intervals;
	unsigned int current_ticket, interval_version;
	uint64_t max_size;

	obj_info(unsigned int id, uint64_t ps) : 
	    last_root(id, 0, ps, ps), current_ticket(1), interval_version(1), max_size(ps)  { }
    };
    rpcreturn_t getIntervalVersion(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t getVersion(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t getTicket(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t create(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t publish(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t get_objcount(const rpcvector_t &params, rpcvector_t &result);

    ~lock_management();
    lock_management();
private:
    typedef __gnu_cxx::hash_map<unsigned int, obj_info> obj_hash_t;

    void compute_sibling_versions(lockmgr_reply::siblings_enum_t &siblings,
				   metadata::query_t &edge_node,
				   obj_info::interval_list_t &intervals, 
				   uint64_t root_size);
    obj_hash_t obj_hash;
    int32_t obj_count;

    config::lock_t mgr_lock;    
};

#endif
