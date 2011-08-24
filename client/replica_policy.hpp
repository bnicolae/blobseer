#ifndef __RANDOM_REPLICA_SELECTION
#define __RANDOM_REPLICA_SELECTION

#include <cstdlib>

#include "common/buffer_wrapper.hpp"
#include "common/structures.hpp"

/// Replica selection policy: random
/**
   This implements a replica selection mechanism for some object. 
   It should encapsulate the version and all replication related
   information. The upper layers will just call try_next(), responsible
   for returning the next replica candidate.
 */
class random_select {
private:
    metadata::replica_list_t providers;
    buffer_wrapper page_key;
    metadata::provider_desc adv;

public:
    random_select() { }

    bool set_providers(const buffer_wrapper &key, buffer_wrapper val) {
	page_key = key;
	return val.size() != 0 && val.getValue(&providers, true);
    }

    buffer_wrapper get_page_key() {
	return page_key;
    }

    metadata::provider_desc try_next() {
	if (providers.size() == 0)
	    return metadata::provider_desc();
	boost::uint32_t rand_idx = rand() % providers.size();
	adv = providers[rand_idx];
	providers.erase(providers.begin() + rand_idx);
	
	return adv;
    }

    metadata::provider_desc try_again() {
	return adv;
    }
};

#endif
