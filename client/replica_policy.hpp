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
    int last_index;
    unsigned int version;

public:
    random_select(vreplica_t rep) : vreplica(rep), last_index(-1) { 
	if (rep->leaf.size() > 0)
	    version = rep->leaf[0].get_free();
	else
	    version = 0;
    }
    random_select() : vreplica(vreplica_t()), last_index(-1), version(0) { }

    provider_adv try_next() {
	if (last_index != -1)
	    vreplica->leaf.erase(vreplica->leaf.begin() + last_index);
	if (vreplica->leaf.size() == 0)
	    return provider_adv();
	last_index = rand() % vreplica->leaf.size();
	return vreplica->leaf[last_index];
    }

    unsigned int get_version() {
	return version;
    }
};

#endif
