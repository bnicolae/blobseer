#include "lock_management.hpp"
#include "lockmgr.hpp"
#include "common/debug.hpp"

lock_management::lock_management() : obj_count(0) {
}

lock_management::~lock_management() {
}

rpcreturn_t lock_management::getIntervalVersion(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }
    metadata::query_t query;
    if (!params[0].getValue(&query, true)) {
	ERROR("RPC error: wrong argument");	
	return rpcstatus::earg;
    } else {
	config::lock_t::scoped_lock lock(mgr_lock);
	obj_hash_t::iterator i = obj_hash.find(query.id);
	if (i != obj_hash.end()) {
	    query.version = i->second.interval_version++;
	    result.push_back(buffer_wrapper(query, true));
	}
    }
    if (result.size() > 0) {
	INFO("RPC success");
	return rpcstatus::ok;
    } else {
	ERROR("RPC error: requested object " << query << " is unheard of");
	return rpcstatus::eobj;
    }
}

rpcreturn_t lock_management::getVersion(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }
    metadata::root_t last_root(0, 0, 0, 0);
    unsigned int id;
    if (!params[0].getValue(&id, true)) {
	ERROR("RPC error: wrong argument");	
	return rpcstatus::earg;
    } else {
	config::lock_t::scoped_lock lock(mgr_lock);
	obj_hash_t::iterator i = obj_hash.find(id);
	if (i != obj_hash.end())
	    last_root = i->second.last_root;
    }
    INFO("RPC success");
    result.push_back(buffer_wrapper(last_root, true));
    return rpcstatus::ok;
}

void lock_management::compute_sibling_versions(lockmgr_reply::siblings_enum_t &siblings,
					       metadata::query_t &edge_node,
					       obj_info::interval_list_t &intervals, 
					       uint64_t root_size) {
    metadata::query_t current_node = edge_node;
    while (current_node.size < root_size) {
	metadata::query_t brother = current_node;
	if (current_node.offset % (2 * current_node.size) == 0)
	    brother.offset = current_node.offset + current_node.size;
	else {
	    brother.offset = current_node.offset - current_node.size;
	    current_node.offset = brother.offset;
	}
	current_node.size *= 2;
	// current node is now the parent, brother is the direct sibling.
	for (obj_info::interval_list_t::reverse_iterator j = intervals.rbegin(); j != intervals.rend(); j++)
	    if (brother.intersects(j->first)) {
		brother.version = j->first.version;
		siblings.push_back(brother);
		break;
	    }
    }
}

rpcreturn_t lock_management::getTicket(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 3) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }
    metadata::query_t query, left, right;
    lockmgr_reply mgr_reply;

    if (!params[0].getValue(&query, true) || !params[1].getValue(&left, true) || !params[2].getValue(&right, true)) {
	ERROR("RPC error: at least one argument is wrong");
	return rpcstatus::earg;
    } else {
	config::lock_t::scoped_lock lock(mgr_lock);
	obj_hash_t::iterator i = obj_hash.find(query.id);
	if (i != obj_hash.end()) {
	    // if the WRITE will actually overflow, let the tree grow.
	    while (query.offset + query.size > i->second.max_size)
		i->second.max_size <<= 1;
	    INFO("max_size is " << i->second.max_size);
	    // reserve a ticket	    
	    mgr_reply.ticket = i->second.current_ticket++;
	    mgr_reply.stable_root = i->second.last_root;
	    mgr_reply.root_size = i->second.max_size;
	    // compute left and right sibling versions
	    compute_sibling_versions(mgr_reply.left, left, i->second.intervals, i->second.max_size);
	    compute_sibling_versions(mgr_reply.right, right, i->second.intervals, i->second.max_size);
	    // insert this interval in the uncompleted interval queue
	    metadata::root_t new_root = i->second.last_root;
	    new_root.node.version = query.version = mgr_reply.ticket;
	    new_root.node.size = i->second.max_size;
	    i->second.intervals.insert(obj_info::interval_entry_t(query, obj_info::root_flag_t(new_root, false)));
	}
    }
    if (mgr_reply.ticket) {
	result.push_back(buffer_wrapper(mgr_reply, true));
	INFO("RPC success");
	return rpcstatus::ok;
    } else {
	ERROR("RPC failed: requested object " << query << " is unheard of");
	return rpcstatus::eobj;
    }
}

rpcreturn_t lock_management::publish(const rpcvector_t &params, rpcvector_t & /*result*/) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }
    metadata::query_t interval(0, 0, 0, 0);
    bool found = false;
    if (!params[0].getValue(&interval, true)) {
	ERROR("RPC error: wrong argument");	
	return rpcstatus::earg;
    } else {
	config::lock_t::scoped_lock lock(mgr_lock);
	obj_hash_t::iterator i = obj_hash.find(interval.id);
	if (i != obj_hash.end()) {
	    obj_info::interval_list_t::iterator j = i->second.intervals.find(interval);
	    // if this publish request has indeed reserved a ticket
	    if (j != i->second.intervals.end()) {
		// mark the interval as completed
		j->second.second = true;
		// all consecutive completed intervals from the beginning will be discarded 
		// and the last one will be the published version
		found = true;
		for (obj_info::interval_list_t::iterator k = i->second.intervals.begin(); 
		     k != i->second.intervals.end() && k->second.second; 
		     k = i->second.intervals.begin()) {
		    i->second.last_root = k->second.first;
		    i->second.intervals.erase(k);
		}		
	    }
	}
    }
    if (!found) {
	ERROR("RPC error: requested object " << interval << " is unheard of");
	return rpcstatus::eobj;
    } else {	
	INFO("RPC success");
	return rpcstatus::ok;
    }    
}

rpcreturn_t lock_management::create(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument number, required: page size");	
	return rpcstatus::earg;
    }

    uint64_t ps;
    if (!params[0].getValue(&ps, true)) {
	ERROR("RPC error: wrong arguments");	
	return rpcstatus::earg;
    } else {
	config::lock_t::scoped_lock lock(mgr_lock);
	
	unsigned int id = ++obj_count;
	obj_info new_obj(id, ps);
	obj_hash.insert(std::pair<unsigned int, obj_info>(id, new_obj));
	result.push_back(buffer_wrapper(new_obj.last_root, true));    
    }
    INFO("RPC success");    
    return rpcstatus::ok;
}

rpcreturn_t lock_management::get_objcount(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 0) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }

    result.push_back(buffer_wrapper(obj_count, true));
    INFO("RPC success");    
    return rpcstatus::ok;
}
