#ifndef __ADV_MANAGER
#define __ADV_MANAGER

#include <map>
#include <vector>

#include "common/config.hpp"
#include "pmanager/publisher.hpp"
#include "provider/provider_adv.hpp"
#include "rpc/rpc_meta.hpp"

class adv_manager {    
private:
    typedef std::pair<unsigned int, unsigned int> adv_info_t;
    typedef std::pair<adv_info_t, provider_adv> free_map_entry_t;
    
    class adv_lessthan {
    public:
	bool operator()(const adv_info_t &s1, const adv_info_t &s2) const {
	    // check if nodes are full
	    if (s1.second == 0) 
		return false;
	    if (s2.second == 0)
		return true;
	    // check work in progress for nodes 
	    if (s1.first < s2.first)
		return true;
	    if (s1.first > s2.first)
		return false;	   
	    // check fullness of nodes
	    return s1.second > s2.second;
	}
    };

    typedef std::multimap<adv_info_t, provider_adv, adv_lessthan> free_map_t;
    typedef std::pair<provider_adv, free_map_t::iterator> adv_hash_entry_t;
    typedef __gnu_cxx::hash_map<provider_adv, free_map_t::iterator, provider_adv_hash> adv_hash_t;
    typedef config::lock_t::scoped_lock scoped_lock_t;
    
public:    
    rpcreturn_t update(const rpcvector_t &params, rpcvector_t &result, const std::string &id);
    rpcreturn_t get(const rpcvector_t &params, rpcvector_t &result);
    ~adv_manager();
    adv_manager();
private:
    free_map_t free_map;
    adv_hash_t adv_hash;
    config::lock_t update_lock;
};

#endif
