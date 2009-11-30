#ifndef __REPLICATION_POLICY
#define __REPLICATION_POLICY

#include <cstdlib>

#include "provider/provider_adv.hpp"
#include "common/structures.hpp"

/// Replica selection policy: random
/**
   This implements a replica selection mechanism for some object. 
   It should encapsulate the version and all replication related
   information. The upper layers will just call try_next(), responsible
   for returning the next replica candidate.
 */
class random_select {
public:
    typedef boost::shared_ptr<metadata::dhtnode_t> vreplica_t;
private:
    vreplica_t vreplica;
    boost::uint32_t index, version;

public:
    random_select(vreplica_t rep) : vreplica(rep) { 
	if (rep->leaf.size() > 0) {
	    version = rep->leaf[0].get_free();
	    index = rep->leaf[0].get_update_rate();
	} else
	    version = index = 0;
    }
    random_select() : vreplica(vreplica_t()), index(0), version(0) { }

    provider_adv try_next() {
	if (vreplica.get() == NULL || vreplica->leaf.size() == 0)
	    return provider_adv();
	boost::uint32_t rand_idx = rand() % vreplica->leaf.size();
	provider_adv adv = vreplica->leaf[rand_idx];
	vreplica->leaf.erase(vreplica->leaf.begin() + rand_idx);
	
	return adv;
    }

    boost::uint32_t get_version() {
	return version;
    }

    boost::uint32_t get_index() {
	return index;
    }
};

#endif
