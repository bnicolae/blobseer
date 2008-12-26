#include "range_query.hpp"
#include "vmanager/main.hpp"

#include "common/debug.hpp"

#include <iostream>

using namespace std;

static const std::string SECRET  = "just something";
static const unsigned int TTL = 86400;

typedef interval_range_query::dht_t dht_t;
typedef boost::shared_ptr<metadata::dhtnode_t> pdhtnode_t;

interval_range_query::interval_range_query(dht_t *dht) {
    this->dht = dht;
}

interval_range_query::~interval_range_query() {
}

// --- WRITE METADATA ----

static void write_callback(bool &result, int error_code) {
    if (error_code != 0)
	result = false;
}

static bool read_node(bool &result, metadata::dhtnode_t &node, buffer_wrapper val) {
    if (!result)
	return false;
    if (val.size() != 0 && val.getValue(&node, true))
	return true;
    result = false;
    return false;
}

static bool read_pnode(bool &result, pdhtnode_t node, buffer_wrapper val) {
    if (!result)
	return false;
    if (val.size() != 0 && val.getValue(node.get(), true))
	return true;
    result = false;
    return false;
}

static void siblings_callback(dht_t *dht, bool isLeft, metadata::query_t &target, 
			      vmgr_reply::siblings_enum_t &siblings, metadata::query_t parent, buffer_wrapper val) {
    metadata::dhtnode_t node;
    bool result = true;
    if (!read_node(result, node, val))
	return;
    if (!node.leaf.empty())
	return;
    node.left.size = node.right.size = parent.size / 2;
    node.left.offset = parent.offset;
    node.right.offset = parent.offset + node.left.size;
    DBG("NODE is: " << node);
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

bool interval_range_query::writeRecordLocations(vmgr_reply &mgr_reply, node_deque_t &node_deque, std::vector<provider_adv> &adv) {
    if (node_deque.empty())
	return false;
    bool result = true;

    DBG("root size = " << mgr_reply.root_size);
    // first write the nodes in the queue
    for (unsigned int i = 0, j = 0; result && (i < node_deque.size()); i++, j += mgr_reply.stable_root.replica_count) {
	metadata::dhtnode_t node;
	node.left = node_deque[i];
	for (unsigned int k = j; result && k < j + mgr_reply.stable_root.replica_count; k++)
	    node.leaf.push_back(adv[k]);
	DBG("PUT: " << node_deque[i]);
	dht->put(buffer_wrapper(node_deque[i], true), 
		 buffer_wrapper(node, true), 
		 TTL, SECRET,
		 bind(write_callback, boost::ref(result), _1)
	    );
    }

    // fill in the left siblings from the stable version if we intresect the stable root, or use it directly if not
    if (mgr_reply.stable_root.node.intersects(node_deque.front()))
	dht->get(buffer_wrapper(mgr_reply.stable_root.node, true), 
		 boost::bind(siblings_callback, dht, true, 
			     boost::ref(node_deque.front()), 
			     boost::ref(mgr_reply.left), 
			     mgr_reply.stable_root.node, _1
		     )
	    );
    else if (vmgr_reply::search_list(mgr_reply.left, 
				     mgr_reply.stable_root.node.offset, 
				     mgr_reply.stable_root.node.size).empty())
	mgr_reply.left.push_back(mgr_reply.stable_root.node);
    
    // fill in the missing right siblings from the stable version (only if it makes sense)
    if (mgr_reply.stable_root.node.intersects(node_deque.back()))
	dht->get(buffer_wrapper(mgr_reply.stable_root.node, true), 
		 boost::bind(siblings_callback, dht, false, 
			     boost::ref(node_deque.back()), 
			     boost::ref(mgr_reply.right), 
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
	    next_node = vmgr_reply::search_list(mgr_reply.right, new_size, first_node.size);
	}
	// if I was a right child, get my left brother
	if (position == metadata::RIGHT_CHILD) {	    
	    uint64_t new_size = first_node.offset - first_node.size;
	    next_node = first_node;
	    first_node = vmgr_reply::search_list(mgr_reply.left, new_size, next_node.size);
	}
	node_deque.push_back(first_parent);
	metadata::dhtnode_t node;
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

static void read_callback(dht_t *dht, metadata::query_t &range, bool &result, 
			  std::vector<interval_range_query::replica_policy_t> &leaves, uint64_t page_size, buffer_wrapper val) {
    pdhtnode_t node = pdhtnode_t(new metadata::dhtnode_t());

    if (!read_pnode(result, node, val))	
	return;
    DBG("READ NODE " << *node);
    if (!node->leaf.empty()) {
	leaves[(node->left.offset - range.offset) / page_size] = interval_range_query::replica_policy_t(node);
	return;
    }
    if (node->left.intersects(range))
	dht->get(buffer_wrapper(node->left, true), 
	    boost::bind(read_callback, dht, boost::ref(range), boost::ref(result), boost::ref(leaves), page_size, _1));
    if (node->right.intersects(range))
	dht->get(buffer_wrapper(node->right, true), 
	    boost::bind(read_callback, dht, boost::ref(range), boost::ref(result), boost::ref(leaves), page_size, _1));
}

bool interval_range_query::readRecordLocations(std::vector<interval_range_query::replica_policy_t> &leaves, metadata::query_t &range, metadata::root_t &root) {
    bool result = true;

    dht->get(buffer_wrapper(root.node, true),
	boost::bind(read_callback, dht, boost::ref(range), boost::ref(result), boost::ref(leaves), root.page_size, _1));
    dht->wait();
    return result;
}
