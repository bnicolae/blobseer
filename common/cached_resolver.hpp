#ifndef __CACHED_RESOLVER
#define __CACHED_RESOLVER

#include <iostream>
#include <boost/bind.hpp>

#include "common/cache_mt.hpp"

typedef std::pair<std::string, std::string> string_pair_t;

// forward declarations are necessary
template <class Transport, class Lock> class cached_resolver; 
template <class Transport, class Lock> std::ostream &operator<<(std::ostream &out, cached_resolver<Transport, Lock> &desc);

/// Cached name resolver
/**
   Transparently allows upper layers to address machines by their names and provided services.
   All DNS requests are solved and cached for quick reuse. Works completely asynchronous: 
   the user provides a custom io_service to register requests with. Registration is bypassed 
   and the handler directly called if the structure could be found in the cache.
 */
template <class Transport, class Lock>
class cached_resolver {
    typedef typename Transport::endpoint endpoint_t;    
    typedef typename boost::function<void (const boost::system::error_code &, endpoint_t)> resolve_callback_t;
    typedef typename Transport::resolver resolver_t;
    typedef typename resolver_t::iterator resolver_iterator_t;
    typedef cache_mt<string_pair_t, endpoint_t, Lock> host_cache_t;

public:
    static const unsigned int CACHE_SIZE = 1 << 16;
    
    /// Constructs a new cached_resolver, assigning it to an io_service
    /**
       Will assign this object to the supplied io_service. 
       All dispatched requests are registered with this io_service
       @param io_service The assigned boost::asio::io_service
       @param cache_size Optionally supply the maximum cache size
    */
    cached_resolver(boost::asio::io_service &io_service, unsigned int cache_size = CACHE_SIZE) {
	this->resolver = new resolver_t(io_service);
	this->host_cache = new host_cache_t(cache_size);
    }

    /// Destroys the cached_resolver
    /** Frees allocated structures */
    ~cached_resolver() { 
	delete resolver;
	delete host_cache;
    }

    /// Dispatch a resolve request to the io_service
    /** 
	Registers a new resolve request with the io_service. The callback will be called when 
	the operation completes. Callback has 2 arguments: error code and endpoint.
	If error code is null endpoint is valid and may be used with asio.
	@param host The hostname of the machine
	@param service The service provided by the machine
	@param callback The callback to be called when the request completes
     */
    void dispatch(const std::string &host, const std::string &service, resolve_callback_t callback);

    /// Pretty print the host cache
    friend std::ostream &operator<< <Transport, Lock> (std::ostream &out, cached_resolver<Transport, Lock> &desc);

private:    
    resolver_t *resolver;
    host_cache_t *host_cache;

    void handle_resolve(const std::string &host, const std::string &service, resolve_callback_t callback,
			const boost::system::error_code& error, resolver_iterator_t it);
};

template <class Transport, class Lock>
void cached_resolver<Transport, Lock>::dispatch(const std::string &host, const std::string &service, resolve_callback_t callback) {
    endpoint_t endpoint;

    if (host_cache->read(string_pair_t(host, service), &endpoint))
	callback(boost::system::error_code(), endpoint);
    else
	resolver->async_resolve(typename resolver_t::query(host, service), 
				boost::bind(&cached_resolver::handle_resolve, this, boost::ref(host), boost::ref(service), callback, _1, _2));
}

template <class Transport, class Lock>
void cached_resolver<Transport, Lock>::handle_resolve(const std::string &host, const std::string &service, resolve_callback_t callback,
						      const boost::system::error_code& error, resolver_iterator_t it) {
    endpoint_t endpoint;
    if (!error) {
	endpoint = *it;
	host_cache->write(string_pair_t(host, service), endpoint);
    }
    callback(error, endpoint);
}

template <class Transport, class Lock> 
std::ostream &operator<<(std::ostream &out, cached_resolver<Transport, Lock> &desc) {
    typedef typename cached_resolver<Transport, Lock>::host_cache_t::const_iterator it_t;

    if (desc.host_cache->size() == 0)
	out << "empty host cache";
    else for (it_t i = desc.host_cache->begin(); i != desc.host_cache->end(); ++i)
	     out << i->first.first << ":" << i->first.second << " -> " << i->second.address().to_string() << ":" << i->second.port() << std::endl;
    return out;
}

#endif
