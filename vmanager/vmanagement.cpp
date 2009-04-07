#include "vmanagement.hpp"
#include "main.hpp"

#include "common/debug.hpp"

vmanagement::vmanagement() : obj_count(0) {
}

vmanagement::~vmanagement() {
}

rpcreturn_t vmanagement::getIntervalVersion(const rpcvector_t &params, rpcvector_t &result) {
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

rpcreturn_t vmanagement::getVersion(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 1) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }
    metadata::root_t last_root(0, 0, 0, 0, 0);
    boost::uint32_t id;
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

void vmanagement::compute_sibling_versions(vmgr_reply::siblings_enum_t &siblings,
					   metadata::query_t &edge_node,
					   obj_info::interval_list_t &intervals, 
					   boost::uint64_t root_size) {
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

rpcreturn_t vmanagement::getTicket(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 2) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }
    metadata::query_t query;
    vmgr_reply mgr_reply;
    bool append;

    if (!params[0].getValue(&query, true) || !params[1].getValue(&append, true)) {
	ERROR("RPC error: at least one argument is wrong");
	return rpcstatus::earg;
    } else {
	config::lock_t::scoped_lock lock(mgr_lock);
	obj_hash_t::iterator i = obj_hash.find(query.id);
	if (i != obj_hash.end()) {
	    boost::uint64_t page_size = i->second.last_root.page_size;
	    
	    // check if we are dealing with an append, adjust offset if it is the case
	    if (append)
		query.offset = i->second.progress_size;

	    // calculate the left and right leaf
	    metadata::query_t left(query.id, query.version, (query.offset / page_size) * page_size, page_size);
	    metadata::query_t right(query.id, query.version,
				    ((query.offset + query.size) / page_size - ((query.offset + query.size) % page_size == 0 ? 1 : 0)) * page_size,
				    page_size);

	    // if the WRITE will actually overflow, let the tree grow
	    while (query.offset + query.size > i->second.max_size)
		i->second.max_size <<= 1;

	    // reserve a ticket.
	    mgr_reply.ticket = i->second.current_ticket++;
	    mgr_reply.stable_root = i->second.last_root;
	    mgr_reply.root_size = i->second.max_size;
	    mgr_reply.append_offset = query.offset;

	    // compute left and right sibling versions.
	    compute_sibling_versions(mgr_reply.left, left, i->second.intervals, i->second.max_size);
	    compute_sibling_versions(mgr_reply.right, right, i->second.intervals, i->second.max_size);

	    // insert this range in the uncompleted range queue.
	    metadata::root_t new_root = i->second.last_root;
	    new_root.node.version = query.version = mgr_reply.ticket;
	    new_root.node.size = i->second.max_size;
	    if (query.offset + query.size > new_root.current_size) {
		new_root.current_size = query.offset + query.size;
		i->second.progress_size = new_root.current_size;
	    }
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

rpcreturn_t vmanagement::publish(const rpcvector_t &params, rpcvector_t & /*result*/) {
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
	    DBG("latest published root = " << i->second.last_root.node << ", current_size = " << i->second.last_root.current_size);
	}
    }
    if (!found) {
	ERROR("RPC error: requested object " << interval << " is unheard of");
	return rpcstatus::eobj;
    } else {	
	return rpcstatus::ok;
    }    
}

rpcreturn_t vmanagement::create(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 2) {
	ERROR("RPC error: wrong argument number, required: page size, replication count");	
	return rpcstatus::earg;
    }

    boost::uint64_t ps;
    boost::uint32_t rc;
    if (!params[0].getValue(&ps, true) || !params[1].getValue(&rc, true)) {
	ERROR("RPC error: wrong arguments");	
	return rpcstatus::earg;
    } else {
	config::lock_t::scoped_lock lock(mgr_lock);
	
	unsigned int id = ++obj_count;
	obj_info new_obj(id, ps, rc);
	obj_hash.insert(std::pair<unsigned int, obj_info>(id, new_obj));
	result.push_back(buffer_wrapper(new_obj.last_root, true));    
    }
    INFO("RPC success");    
    return rpcstatus::ok;
}

rpcreturn_t vmanagement::get_objcount(const rpcvector_t &params, rpcvector_t &result) {
    if (params.size() != 0) {
	ERROR("RPC error: wrong argument number");	
	return rpcstatus::earg;
    }

    result.push_back(buffer_wrapper(obj_count, true));
    INFO("RPC success");    
    return rpcstatus::ok;
}
