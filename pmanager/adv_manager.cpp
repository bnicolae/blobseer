 #include "adv_manager.hpp"
#include "common/debug.hpp"

adv_manager::adv_manager() {
}

adv_manager::~adv_manager() {
}

rpcreturn_t adv_manager::update(const rpcvector_t &params, rpcvector_t & /*result*/, const std::string &id) {
    if (params.size() != 1) {
	ERROR("adv_manager::update(): RPC error: wrong argument number: " << params.size());
	return rpcstatus::earg;
    }
    provider_adv adv;
    if (!params[0].getValue(&adv, true)) {
	ERROR("adv_manager::update(): RPC error: wrong argument");
	return rpcstatus::earg;
    } else {
	adv.set_host(id);
	unsigned int score = 0;
	{
	    scoped_lock_t lock(update_lock);
	    // erase previous adv data
	    adv_hash_t::const_iterator it = adv_hash.find(adv);
	    if (it != adv_hash.end()) {
		free_map_t::iterator fit = it->second;
		// let's skip state updates for now and stick to round robin for more predictability
		score = fit->first.first; //- adv.get_update_rate();
		INFO("old score = " << fit->first.first << ", score = " << score << ", value = {" << score - fit->first.first << "} (PWR)");
		free_map.erase(fit);		
	    }
	    // insert new adv data
	    free_map_t::iterator fit = free_map.insert(free_map_entry_t(adv_info_t(score, adv.get_free()), adv));
	    adv_hash.erase(adv);
	    adv_hash.insert(adv_hash_entry_t(adv, fit));
	}
	INFO("Updated advertisement " << adv << ": score decreased, now is " << score);
	return rpcstatus::ok;
    }
}

rpcreturn_t adv_manager::get(const rpcvector_t &params, rpcvector_t & result) {
    if (params.size() != 1) {
	ERROR("adv_manager::get(): RPC error: wrong argument number");	
	return rpcstatus::earg;
    } 
    unsigned int no_providers = 0;
    if (!params[0].getValue(&no_providers, true) || no_providers == 0) {
	ERROR("adv_manager::get(): RPC error: wrong argument");
	return rpcstatus::earg;
    } else {
	std::vector<provider_adv> adv_list = std::vector<provider_adv>();
	{
	    scoped_lock_t lock(update_lock);
	    while (adv_list.size() < no_providers) {
		free_map_t::iterator fit = free_map.begin(); 
		// provider is full
		if (fit->first.second == 0)
		    break;
		unsigned int score = fit->first.first + 1;
		adv_list.push_back(fit->second);
		free_map.erase(fit);
		fit = free_map.insert(free_map_entry_t(adv_info_t(score, adv_list.back().get_free()), adv_list.back()));
		DBG("Updated advertisement " << adv_list.back() << ": score increased, now is " << score);		
		adv_hash.erase(adv_list.back());
		adv_hash.insert(adv_hash_entry_t(adv_list.back(), fit));
	    }
	}
	if (adv_list.size() != no_providers) {
	    ERROR("adv_manager::get(): could not satisfy request, not enough suitable providers");
	    return rpcstatus::eres;
	} else {	    
	    INFO("adv_manager::get(): RPC success, now sending the advertisement list, size is: " << adv_list.size());
	    result.push_back(buffer_wrapper(adv_list, true));
	    return rpcstatus::ok;
	}
    }
}
