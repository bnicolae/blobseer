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

#ifndef __ADV_MANAGER
#define __ADV_MANAGER

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>

#include "common/config.hpp"
#include "common/null_lock.hpp"
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
	
	boost::uint64_t score, free, read_pages, read_total;
	
	score_entry(boost::uint64_t s, boost::uint64_t f, boost::uint64_t rp, boost::uint64_t rt) 
	    : score(s), free(f), read_pages(rp), read_total(rt) { }
	
	/// Compare providers - original strategy
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

	/// Compare providers - machine learning hinted strategy by applying GloBeM
	/**
	   This strategy is fit for specific MapReduce workloads, described in recent work.
	   The winning provider is the one with the lowest score, but only if free space
	   is still available on it and there are less than 6 read operations / second to it.
	*/
	/*
	bool operator<(const score_entry &s) const {
	    if (free == 0)
		return false;
	    if (s.free == 0)
		return true;
	    if (read_pages > 30)
		return false;
	    if (s.read_pages > 30)
		return true;
	    if (score < s.score)
		return true;
	    if (score > s.score)
		return false;
	    return free > s.free;
	}
	*/
    };
	
    class table_entry {
    public: 	
	string_pair_t id;
	score_entry info;
	boost::posix_time::ptime timestamp;

	table_entry(const string_pair_t &a, const score_entry &s) : 
	    id(a), info(s), timestamp(boost::posix_time::microsec_clock::local_time()) { }
	table_entry(const string_pair_t &a) : 
	    id(a), info(score_entry(0, 0, 0, 0)), timestamp(boost::posix_time::microsec_clock::local_time()) { }
    };

    // define the tags and the multi-index
    struct tid {};
    struct tinfo {};
    struct ttime {};
    typedef boost::multi_index_container<
	table_entry,
	boost::multi_index::indexed_by <
	    boost::multi_index::hashed_unique<
		boost::multi_index::tag<tid>, BOOST_MULTI_INDEX_MEMBER(table_entry, string_pair_t, id)
		>,
	    boost::multi_index::ordered_non_unique<
		boost::multi_index::tag<tinfo>, BOOST_MULTI_INDEX_MEMBER(table_entry, score_entry, info)
		>,
	    boost::multi_index::ordered_non_unique<
		boost::multi_index::tag<ttime>, BOOST_MULTI_INDEX_MEMBER(table_entry, boost::posix_time::ptime, timestamp)
		> 
	    >
	> adv_table_t;
    
    typedef null_lock::scoped_lock scoped_lock_t;

    typedef boost::multi_index::index<adv_table_t, tid>::type adv_table_by_id;
    typedef boost::multi_index::index<adv_table_t, tinfo>::type adv_table_by_info;
    typedef boost::multi_index::index<adv_table_t, ttime>::type adv_table_by_time;

    static const unsigned int WATCHDOG_TIMEOUT = 120;
    
public:    
    rpcreturn_t update(const rpcvector_t &params, rpcvector_t &result, const std::string &id);
    rpcreturn_t get(const rpcvector_t &params, rpcvector_t &result);
    ~adv_manager();
    adv_manager();
    
private:
    adv_table_t adv_table;
    null_lock update_lock;
    boost::thread watchdog;

    void watchdog_exec();
};

#endif
