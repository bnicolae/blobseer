#ifndef __ADV_MANAGER
#define __ADV_MANAGER

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>

#include "common/config.hpp"
#include "pmanager/publisher.hpp"
#include "provider/provider_adv.hpp"
#include "rpc/rpc_meta.hpp"

class adv_manager {    
private:
    
    /// Keep scores for each provider
    /** 
	A score is assigned to each provider. Free space available on 
	each provider is also stored.
    */

    class score_entry {
    public:
	typedef std::pair<boost::uint64_t, boost::uint64_t> score_t;
	
	boost::uint64_t score, free;
	
	score_entry(boost::uint64_t s, boost::uint64_t f) : score(s), free(f) { }
	
	/// Compare providers
	/**
	   The winning provider is the one with the lowest score, but only if free space
	   is still available on it. If the score is the same, then the provider with the
	   most free space available will win.
	 */
	bool operator<(const score_entry &s) const {	   
	    if (free == 0 && s.free == 0)
		return score < s.score;
	    if (free == 0 || score > s.score)
		return false;
	    if (s.free == 0 || score < s.score)
		return true;
	    return free > s.free;
	}
    };
	
    class table_entry {
    public: 	
	string_pair_t id;
	score_entry info;

	table_entry(const string_pair_t &a, const score_entry &s) : id(a), info(s) { }
	table_entry(const string_pair_t &a) : id(a), info(score_entry(0, 0)) { }
    };

    // define the tags and the multi-index
    struct tid {};
    struct tinfo {};
    typedef boost::multi_index_container<
	table_entry,
	boost::multi_index::indexed_by <
	    boost::multi_index::hashed_unique<
		boost::multi_index::tag<tid>, BOOST_MULTI_INDEX_MEMBER(table_entry, string_pair_t, id)
		>,
	    boost::multi_index::ordered_non_unique<
		boost::multi_index::tag<tinfo>, BOOST_MULTI_INDEX_MEMBER(table_entry, score_entry, info)
		> 
	    >
	> adv_table_t;
    
    typedef config::lock_t::scoped_lock scoped_lock_t;

    typedef boost::multi_index::index<adv_table_t, tid>::type adv_table_by_id;
    typedef boost::multi_index::index<adv_table_t, tinfo>::type adv_table_by_info;
    
public:    
    rpcreturn_t update(const rpcvector_t &params, rpcvector_t &result, const std::string &id);
    rpcreturn_t get(const rpcvector_t &params, rpcvector_t &result);
    ~adv_manager();
    adv_manager();
    
private:
    adv_table_t adv_table;
    config::lock_t update_lock;
};

#endif
