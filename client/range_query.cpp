#include "range_query.hpp"
#include "vmanager/main.hpp"

#include "common/debug.hpp"

static const std::string SECRET  = "just something";
static const unsigned int TTL = 86400;

typedef interval_range_query::dht_t dht_t;

interval_range_query::interval_range_query(dht_t *dht_) : dht(dht_) { }

interval_range_query::~interval_range_query() { }

// --- WRITE METADATA ----

static void write_callback(bool &result, int error_code) {
    if (error_code != 0)
	result = false;
}

static inline bool read_node(metadata::dhtnode_t &node, buffer_wrapper val) {
    return val.size() != 0 && val.getValue(&node, true);
}

static bool read_pnode(bool &result, metadata::dhtnode_t &node, buffer_wrapper val) {
    if (!result)
	return false;
    if (val.size() != 0 && val.getValue(&node, true))
	return true;
    result = false;
    return false;
}

static void siblings_callback(dht_t *dht, bool isLeft, metadata::query_t &target, 
			      metadata::siblings_enum_t &siblings, metadata::query_t parent, buffer_wrapper val) {
    metadata::dhtnode_t node(false);

    if (!read_node(node, val))
	return;
    if (node.is_leaf)
	return;
    node.left.size = node.right.size = parent.size / 2;
    node.left.offset = parent.offset;
    node.right.offset = parent.offset + node.left.size;
    DBG("NODE is: " << node << ", TARGET is: " << target << ", isLeft is: " 
	<< isLeft << ", left intersects = " << node.left.intersects(target));
    if (node.left.intersects(target)) {
	if (!isLeft && vmgr_reply::search_list(siblings, node.right.offset, node.right.size).empty()) {
	    DBG("PUSH RIGHT SIBLING: " << node.right);
	    siblings.push_back(node.right);
	}
	DBG("TAKING LEFT: " << node.left);
	dht->get(buffer_wrapper(node.left, true),
		 boost::bind(siblings_callback, dht, isLeft, boost::ref(target), boost::ref(siblings), node.left, _1));
    } else {
	if (isLeft && vmgr_reply::search_list(siblings, node.left.offset, node.left.size).empty()) {
	    DBG("PUSH LEFT SIBLING: " << node.left);
	    siblings.push_back(node.left);
	}
	DBG("TAKING RIGHT: " << node.right);
	dht->get(buffer_wrapper(node.right, true), 
		 boost::bind(siblings_callback, dht, isLeft, boost::ref(target), boost::ref(siblings), node.right, _1));
    }
}

static void compute_sibling_versions(metadata::siblings_enum_t &siblings,
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

bool interval_range_query::writeRecordLocations(vmgr_reply &mgr_reply, node_deque_t &node_deque, metadata::replica_list_t &provider_list) {
    if (node_deque.empty())
	return false;
    bool result = true;
    metadata::query_t query = mgr_reply.intervals.rbegin()->first;

    DBG("root size = " << mgr_reply.root_size);
    // first write the nodes in the queue
    for (unsigned int i = 0, j = 0; result && (i < node_deque.size()); i++, j += mgr_reply.stable_root.replica_count) {
	metadata::dhtnode_t node(true);
	metadata::replica_list_t providers;

	node.left = node_deque[i];
	node_deque[i].version = query.version;
	node_deque[i].offset = query.offset + node_deque[i].offset * node_deque[i].size;
	for (unsigned int k = j; result && k < j + mgr_reply.stable_root.replica_count; k++)
	    providers.push_back(provider_list[k]);
	// put list of providers
	DBG("PUT PAGE KEY: " << node.left);
	dht->put(buffer_wrapper(node.left, true), 
		 buffer_wrapper(providers, true), 
		 TTL, SECRET,
		 bind(write_callback, boost::ref(result), _1)
	    );
	DBG("PUT LEAF: " << node_deque[i]);
	dht->put(buffer_wrapper(node_deque[i], true), 
		 buffer_wrapper(node, true), 
		 TTL, SECRET,
		 bind(write_callback, boost::ref(result), _1)
	    );
    }

    // calculate the left and right leaf
    boost::uint64_t page_size = mgr_reply.stable_root.page_size;
    metadata::query_t left(query.id, query.version, (query.offset / page_size) * page_size, page_size);
    metadata::query_t right(query.id, query.version,
			    ((query.offset + query.size) / page_size - 
			     ((query.offset + query.size) % page_size == 0 ? 1 : 0)) * page_size, page_size);
        
    // compute left and right sibling versions.
    metadata::siblings_enum_t left_siblings, right_siblings;

    mgr_reply.intervals.erase(mgr_reply.intervals.rbegin()->first);
    compute_sibling_versions(left_siblings, left, mgr_reply.intervals, 
			     mgr_reply.root_size);
    compute_sibling_versions(right_siblings, right, mgr_reply.intervals, 
			     mgr_reply.root_size);
                
    // fill in the left siblings from the stable version if we intresect the stable root, or use it directly if not
    DBG("stable root: " << mgr_reply.stable_root.node);
    if (mgr_reply.stable_root.node.intersects(node_deque.front()))
	dht->get(buffer_wrapper(mgr_reply.stable_root.node, true), 
		 boost::bind(siblings_callback, dht, true, 
			     boost::ref(node_deque.front()),
			     boost::ref(left_siblings), 
			     mgr_reply.stable_root.node, _1
		     )
	    );
    else if (vmgr_reply::search_list(left_siblings, 
				     mgr_reply.stable_root.node.offset, 
				     mgr_reply.stable_root.node.size).empty())
	left_siblings.push_back(mgr_reply.stable_root.node);
    
    // fill in the missing right siblings from the stable version (only if it makes sense)
    if (mgr_reply.stable_root.node.intersects(node_deque.back()))
	dht->get(buffer_wrapper(mgr_reply.stable_root.node, true), 
		 boost::bind(siblings_callback, dht, false, 
			     boost::ref(node_deque.back()), 
			     boost::ref(right_siblings), 
			     mgr_reply.stable_root.node, _1
		     )
	    );

    dht->wait();

    // process the nodes
    while (result && !node_deque.empty()) {
	metadata::query_t first_node = node_deque.front();
	node_deque.pop_front();
	if (first_node.size == mgr_reply.root_size)
	    continue;
	metadata::query_t first_parent;
	unsigned int position = first_node.getParent(first_parent);
	metadata::query_t next_node;
	// if the next node has the same parent with first_node, elliminate both
	if (!node_deque.empty()) {
	    next_node = node_deque.front();
	    metadata::query_t next_parent;
	    if (next_node.size != mgr_reply.root_size
		&& next_node.getParent(next_parent) == metadata::RIGHT_CHILD
		&& first_parent == next_parent) {
		position = metadata::ROOT;
		node_deque.pop_front();
	    }	    
	}
	// if I was a left child, get my right brother
	if (position == metadata::LEFT_CHILD) {
	    uint64_t new_size = first_node.offset + first_node.size;
	    next_node = vmgr_reply::search_list(right_siblings, new_size, first_node.size);
	}
	// if I was a right child, get my left brother
	if (position == metadata::RIGHT_CHILD) {	    
	    uint64_t new_size = first_node.offset - first_node.size;
	    next_node = first_node;
	    first_node = vmgr_reply::search_list(left_siblings, new_size, next_node.size);
	}
	node_deque.push_back(first_parent);
	metadata::dhtnode_t node(false);
	node.left = first_node;
	node.right = next_node;
	DBG("PUT: " << first_parent << " -> " << node);
	dht->put(buffer_wrapper(first_parent, true), 
		 buffer_wrapper(node, true), 
		 TTL, SECRET,
		 bind(write_callback, boost::ref(result), _1)
	    );

    }
    dht->wait();
    return result;
}
 
// ---- READ METADATA ----
static void leaf_callback(bool &result, 
			  std::vector<interval_range_query::replica_policy_t> &leaves, 
			  metadata::query_t page_key,
			  unsigned int index,
			  buffer_wrapper val) {
    if (!result)
	return;
    ASSERT(index < leaves.size());
    if (!leaves[index].set_providers(page_key, val)) {
	DBG("provider list read fail for page " << page_key);
	result = false;
	return;
    }
}

static void read_callback(dht_t *dht, metadata::query_t &range, bool &result,
			  std::vector<interval_range_query::replica_policy_t> &leaves,
			  boost::uint64_t page_size, boost::uint64_t offset,
			  buffer_wrapper val) {
    metadata::dhtnode_t node(false);

    if (!read_pnode(result, node, val))	
	return;
    DBG("READ NODE " << node);
    if (node.is_leaf) {
	dht->get(buffer_wrapper(node.left, true), 
		 boost::bind(leaf_callback, boost::ref(result), 
			     boost::ref(leaves), node.left, 
			     (offset - range.offset) / page_size, _1));
	return;
    }
    if (node.left.intersects(range))
	dht->get(buffer_wrapper(node.left, true),
	    boost::bind(read_callback, dht, boost::ref(range), boost::ref(result),
			boost::ref(leaves), page_size, node.left.offset, _1));
    if (node.right.intersects(range))
	dht->get(buffer_wrapper(node.right, true),
	    boost::bind(read_callback, dht, boost::ref(range), boost::ref(result),
			boost::ref(leaves), page_size, node.right.offset, _1));
}

bool interval_range_query::readRecordLocations(std::vector<interval_range_query::replica_policy_t> &leaves, metadata::query_t &range, metadata::root_t &root) {
    bool result = true;

    dht->get(buffer_wrapper(root.node, true),
	boost::bind(read_callback, dht, boost::ref(range), boost::ref(result), 
		    boost::ref(leaves), root.page_size, 0, _1));
    dht->wait();
    return result;
}
