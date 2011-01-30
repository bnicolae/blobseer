#include "meta_listener.hpp"
#include "common/debug.hpp"

meta_listener::meta_listener() {
}

meta_listener::~meta_listener() {        
}

/*
  index of fields for monitored_params_t:
  0: free space
  1: key
  2: value
  3: sender
*/
void meta_listener::update_event(const boost::int32_t name, monitored_params_t &params) {
    metadata::dhtnode_t meta_node(false);
    
    switch (name) {    
    case PROVIDER_READ:
	params.get<2>().getValue(&meta_node, true);
	meta_node.access_count++;
	params.get<2>().swap(buffer_wrapper(&meta_node, true));
	break;
    case PROVIDER_WRITE:
	params.get<2>().getValue(&meta_node, true);
	meta_node.access_count = 0;
	params.get<2>().swap(buffer_wrapper(&meta_node, true));
	break;
    default:
	ERROR("Unknown hook type: " << name);
    }
}
