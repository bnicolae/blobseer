#ifndef __SIMPLE_DHT
#define __SIMPLE_DHT

#include "common/null_lock.hpp"
#include "common/config.hpp"
#include "rpc/rpc_client.hpp"
#include "provider/provider.hpp"

template <class SocketType>
class simple_dht {
public:
    // export the params
    typedef SocketType transport_t;
    typedef null_lock lock_t;
    typedef buffer_wrapper pkey_t;
    typedef buffer_wrapper pvalue_t;
    typedef boost::shared_ptr<unsigned int> puint;

    // TO DO: throw exceptions form DHT, capture them and modify to "void (const boost::system::error_code &, int)"
    typedef boost::function<void (int)> mutate_callback_t;
    typedef boost::function<void (pvalue_t)> get_callback_t;
    typedef boost::function<void (int, pvalue_t)> handle_get_callback_t;

    typedef rpc_client<SocketType> rpc_client_t;    
    typedef boost::shared_ptr<std::vector<handle_get_callback_t> > get_callbacks_t;
    typedef boost::shared_ptr<std::vector<mutate_callback_t> > put_callbacks_t;    

    simple_dht(boost::asio::io_service &io_service, unsigned int r = 1, unsigned int timeout = 10);
    ~simple_dht();
    void addGateway(const std::string &host, const std::string &service);
    void put(pkey_t key, pvalue_t value, int ttl, const std::string &secret, mutate_callback_t put_callback);    
    void get(pkey_t key, get_callback_t get_callback, unsigned int n = 0);
    void remove(pkey_t key, pvalue_t value, int ttl, const std::string &secret, mutate_callback_t remove_callback);
    void wait();

private:
    class gateway_t {
    public:
	std::string host, service;	
	rpcvector_t pending_gets, pending_puts;
	get_callbacks_t get_callbacks;
	put_callbacks_t put_callbacks;
	gateway_t(const std::string &h, const std::string &s) :
	    host(h), service(s), 
	    get_callbacks(new std::vector<handle_get_callback_t>), put_callbacks(new std::vector<mutate_callback_t>) { }
    };

    rpc_client_t tp;
    std::vector<gateway_t> gateways;
    unsigned int replication;
    static const unsigned int STATUS_SHIFT = 16, STATUS_MASK = 0xFFFF;

    unsigned int choose_gateway(pkey_t key, unsigned int n = 0);
    void handle_put_callback(puint status, mutate_callback_t put_callback, int error);
    void handle_put_callbacks(const simple_dht::put_callbacks_t &puts, const rpcreturn_t &error, const rpcvector_t &result);
    void handle_get_callback(unsigned int i, pkey_t key, get_callback_t get_callback, int error, buffer_wrapper val);
    void handle_get_callbacks(const simple_dht::get_callbacks_t &gets, const rpcreturn_t &error, const rpcvector_t &result);
};

template <class SocketType>
void simple_dht<SocketType>::handle_put_callback(puint status,
						 mutate_callback_t put_callback,
						 int error) {
    if (error == rpcstatus::ok)
	(*status)++;
    (*status) += 1 << STATUS_SHIFT;
    if ((*status) >> STATUS_SHIFT == replication) {
	if (((*status) & STATUS_MASK) > 0)
	    put_callback(rpcstatus::ok);
	else
	    put_callback(rpcstatus::eobj);
    }
}

template <class SocketType>
void simple_dht<SocketType>::handle_put_callbacks(const simple_dht::put_callbacks_t &puts,
						  const rpcreturn_t &error,
						  const rpcvector_t &/*result*/) {
    for (unsigned int i = 0; i < puts->size(); i++)
	(*puts)[i](error);
}

template <class SocketType>
void simple_dht<SocketType>::handle_get_callback(unsigned int i, pkey_t key, get_callback_t get_callback, 
						 int error, buffer_wrapper val) {
    if (error == rpcstatus::ok || error == rpcstatus::eobj)
	get_callback(val);
    else {
	if (i + 1 == replication)
	    get_callback(buffer_wrapper());
	else
	    get(key, get_callback, i + 1);
    }
}


template <class SocketType>
void simple_dht<SocketType>::handle_get_callbacks(const simple_dht::get_callbacks_t &gets,
						  const rpcreturn_t &error,
						  const rpcvector_t &result) {
    if (gets->size() != result.size()) {
	for (unsigned int i = 0; i < gets->size(); i++)
	    (*gets)[i]((int)error, buffer_wrapper());
    } else {
	for (unsigned int i = 0; i < gets->size(); i++)
	    (*gets)[i]((int)error, result[i]);
    }
}

template <class SocketType>
void simple_dht<SocketType>::wait() {
    bool completed = false;

    while(!completed) {
	completed = true;
	for (unsigned int i = 0; i < gateways.size(); i++) {
	    if (gateways[i].pending_gets.size() > 0) {
		completed = false;
		rpcvector_t gets;
		get_callbacks_t callbacks(new std::vector<handle_get_callback_t>);
		gets.swap(gateways[i].pending_gets);
		callbacks.swap(gateways[i].get_callbacks);

		tp.dispatch(gateways[i].host, gateways[i].service, 
			    PROVIDER_READ, gets,
			    boost::bind(&simple_dht<SocketType>::handle_get_callbacks, this, callbacks, _1, _2));
	    }
	    if (gateways[i].pending_puts.size() > 0) {
		completed = false;
		rpcvector_t puts;
		put_callbacks_t callbacks(new std::vector<mutate_callback_t>);
		puts.swap(gateways[i].pending_puts);
		callbacks.swap(gateways[i].put_callbacks);

		tp.dispatch(gateways[i].host, gateways[i].service, 
			    PROVIDER_WRITE, puts,
			    boost::bind(&simple_dht<SocketType>::handle_put_callbacks, this, callbacks, _1, _2));

	    }
	}
	tp.run();
    }
}

template <class SocketType>
simple_dht<SocketType>::simple_dht(boost::asio::io_service &io_service, unsigned int r, unsigned int /*t*/) : 
    tp(io_service), replication(r) { }

template <class SocketType>
simple_dht<SocketType>::~simple_dht() { }

template <class SocketType>
void simple_dht<SocketType>::addGateway(const std::string &host, const std::string &service) {
    gateways.push_back(gateway_t(host, service));
}

template <class SocketType>
	unsigned int simple_dht<SocketType>::choose_gateway(pkey_t key, unsigned int n) {
    unsigned int hash = n;
    unsigned char *k = (unsigned char *)key.get();

    // sdbm hashing used here
    for (unsigned int i = 0; i < key.size(); i++, k++)
	hash = *k + (hash << 6) + (hash << 16) - hash;
    
    return hash % gateways.size();
}

template <class SocketType>
void simple_dht<SocketType>::put(pkey_t key, pvalue_t value, int /*ttl*/, 
				      const std::string &/*secret*/, mutate_callback_t put_callback) {
    puint status(new unsigned int);
    *status = 0;
	
    for (unsigned int i = 0; i < replication; i++) {
	unsigned int index = choose_gateway(key, i);
	gateways[index].pending_puts.push_back(key);
	gateways[index].pending_puts.push_back(value);
	gateways[index].put_callbacks->push_back(
	    boost::bind(&simple_dht<SocketType>::handle_put_callback, this, status, put_callback, _1)
	    );
    }
}

template <class SocketType>
void simple_dht<SocketType>::remove(pkey_t /*key*/, pvalue_t /*value*/, int /*ttl*/, 
					 const std::string &/*secret*/, mutate_callback_t remove_callback) {
    // not yet implemented
    remove_callback(0);
}

template <class SocketType>
void simple_dht<SocketType>::get(pkey_t key, get_callback_t get_callback, unsigned int n) {
    unsigned int index = choose_gateway(key, n);
    gateways[index].pending_gets.push_back(key);
    gateways[index].get_callbacks->push_back(
	boost::bind(&simple_dht<SocketType>::handle_get_callback, this, n, key, get_callback, _1, _2)
	);
}

#endif
