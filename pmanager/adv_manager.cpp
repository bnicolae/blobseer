#include "adv_manager.hpp"

#include "common/debug.hpp"

adv_manager::adv_manager() : watchdog(boost::thread(boost::bind(&adv_manager::watchdog_exec, this))) { }

adv_manager::~adv_manager() {
    watchdog.interrupt();
    watchdog.join();
}

void adv_manager::watchdog_exec() {
    boost::xtime xt;
    adv_table_by_time &time_index = adv_table.get<ttime>();

    while (1) {	
	boost::xtime_get(&xt, boost::TIME_UTC);
	xt.sec += WATCHDOG_TIMEOUT;
	boost::thread::sleep(xt);

	boost::posix_time::ptime now(boost::posix_time::microsec_clock::local_time());	
	{
	    boost::this_thread::disable_interruption di;
	    scoped_lock_t lock(update_lock);

	    for (adv_table_by_time::iterator ai = time_index.begin(); ai != time_index.end(); ai = time_index.begin()) {
		table_entry e = *ai;
		if (e.timestamp < now) {
		    time_index.erase(ai);

		    INFO("WATCHDOG: provider " << e.id.first << ":" << e.id.second << 
			 " has not reported in " << WATCHDOG_TIMEOUT << " seconds and was blacklisted");
		} else
		    break;
	    }
	}
    }
}

rpcreturn_t adv_manager::update(const rpcvector_t &params, rpcvector_t & /*result*/, const std::string &id) {
    if (params.size() != 2) {
	ERROR("adv_manager::update(): RPC error: wrong argument number: " << params.size());
	return rpcstatus::earg;
    }
    boost::uint64_t free_space;
    std::string service;
    if (!params[0].getValue(&free_space, true) || !params[1].getValue(&service, true)) {
	ERROR("adv_manager::update(): RPC error: wrong argument(s)");
	return rpcstatus::earg;
    } else {
	scoped_lock_t lock(update_lock);
	
	// update free space in the multi-index
	unsigned int pos = id.find(":");
	adv_table_by_id &id_index = adv_table.get<tid>();
	adv_table_by_id::iterator ai = id_index.insert(string_pair_t(id.substr(0, pos), service)).first;
	table_entry e = *ai;

	e.timestamp = boost::posix_time::microsec_clock::local_time() + boost::posix_time::seconds(WATCHDOG_TIMEOUT);
	if (e.info.free != free_space) {
	    e.info.free = free_space;
	    INFO("Updated info for provider (" << e.id.first << ":" << e.id.second << "): (score, free) = " 
		 << e.info.score << ", " << e.info.free);
	}

	id_index.replace(ai, e);	
	return rpcstatus::ok;
    }
}

rpcreturn_t adv_manager::get(const rpcvector_t &params, rpcvector_t & result) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument number; expected = 1; got = " << params.size());
	return rpcstatus::earg;
    }
    unsigned int no_providers = 0;
    if (!params[0].getValue(&no_providers, true) || no_providers == 0) {
	ERROR("RPC error: wrong argument");
	return rpcstatus::earg;
    } else {
	std::vector<provider_adv> adv_list = std::vector<provider_adv>();
	{
	    scoped_lock_t lock(update_lock);

	    while (adv_list.size() < no_providers) {	      
		adv_table_by_info &info_index = adv_table.get<tinfo>();
		adv_table_by_info::iterator ai = info_index.begin();
		table_entry e = *ai;

		if (e.info.free == 0)
		    break;
		e.info.score++;
		info_index.replace(ai, e);

		adv_list.push_back(provider_adv(e.id.first, e.id.second, e.info.free, 0));
		DBG("Allocated provider " << id << ": (score, free) = " << e.info.score << ", " << e.info.free);
	    }
	}
	if (adv_list.size() != no_providers) {
	    ERROR("could not satisfy request, not enough suitable providers; requested = " 
		  << no_providers << "; got = " << adv_list.size());
	    return rpcstatus::eres;
	} else {
	    INFO("RPC success, now sending the advertisement list, size is: " << adv_list.size());
	    result.push_back(buffer_wrapper(adv_list, true));
	    return rpcstatus::ok;
	}
    }
}
