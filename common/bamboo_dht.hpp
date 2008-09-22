#ifndef __BAMBOO_DHT_
#define __BAMBOO_DHT_

#include <vector>
#include <openssl/sha.h>

#include "buffer_wrapper.hpp"
#include "gateway_prot.h"
#include "cached_resolver.hpp"

static const unsigned int VALUE_SIZE_MAX = 1024;
static const unsigned int HASH_LEN = 20;
static char *const APP_STRING = "DHT C++ lib"; 
static char *const CLIB_STRING = "rpcgen";
static char *const HASH_DESC = "SHA1";

#define E_RPC_FAILURE          -1
#define E_MAXVALUE_EXCEEDED    -2

/// Bamboo DHT access
/** 
    Implements the generic DHT functionality on top of Bamboo DHT.
*/
class bamboo_dht {    
    typedef cached_resolver<config::socket_namespace, config::lock_t> cached_resolver_t;
    int retry_count;
    int time_out;

    /// Get a standard SUN RPC CLIENT structure to be used for RPC calls
    /** 
	Randomly selects a gateway and tries to establish connection 
	for maximum of retry_count times. Stops when the attempt is successful.
	@param no Will be set to the number of retries left for subsequent operations after the connection 
	has been established
	@return The CLIENT structure
    */
    CLIENT *getRPCConnection(int &no, int &gid);
public:    
    typedef buffer_wrapper pkey_type;
    typedef buffer_wrapper pvalue_type;
    //typedef boost::shared_ptr<buffered_enumeration<pvalue_type> > enumeration_type;
    /// Constructor
    /** 
	We need to know of at least one node that is part of the DHT
	@param g The node that is part of the DHT
	@param rc Max number of gateways to try before giving up
	@param to Timeout in seconds for the RPC call, default is 5 seconds
    */
    bamboo_dht(int rc = 3, int to = 5);
    /// Destructor
    /** Do nothing for now ... just rely on the destructor of the base class */
    ~bamboo_dht();
    /// Add further nodes that are part of the DHT for efficient querying    
    /** 
	RPC calls are passed to a random nodes to improve load balancing. The more gateways we know of, the better.
	@param g The node that is part of the DHT 
    */
    void addGateway(const std::string &host, const std::string &service);
    /// Put functionality
    /** 	
	See abstract DHT description
    */
    int put(pkey_type key, pvalue_type value, int ttl, const std::string &secret);
    /// Get the a value matching key
    /**       
       Gets the value matching the key. If there are more values matching the key, no
       guarantees are given which one is returned.
       @param key The key
       @return Pointer to the value, NULL on failure
    */
    pvalue_type getValue(pkey_type key);

    /// Remove functionality
    /**
       See abstract DHT description
    */
    int remove(pkey_type key, pvalue_type value, int ttl, const std::string &secret);
};

#endif
