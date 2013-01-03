/**
 * Copyright (C) 2008-2012 Bogdan Nicolae <bogdan.nicolae@inria.fr>
 *
 * This file is part of BlobSeer, a scalable distributed big data
 * store. Please see README (root of repository) for more details.
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses>.
 */

#ifndef __CACHED_DHT
#define __CACHED_DHT

#include <boost/function.hpp>

#include "cache_mt.hpp"

template<class DHT, class HashFcn>
class cached_dht {
public:      
    typedef typename DHT::pkey_t pkey_t;
    typedef typename DHT::pvalue_t pvalue_t;    
    typedef typename DHT::mutate_callback_t mutate_callback_t;
    typedef typename DHT::get_callback_t get_callback_t;
    typedef typename DHT::lock_t lock_t;

    cached_dht(boost::asio::io_service &io_service, unsigned int r = 1, unsigned int t = 10, 
	       unsigned int size = 1 << 20);
    ~cached_dht();
    void addGateway(const std::string &host, const std::string &service);
    void put(pkey_t key, pvalue_t value, int ttl, const std::string &secret, mutate_callback_t put_callback);
    void get(pkey_t key, get_callback_t get_callback);
    void probe(pkey_t key, get_callback_t get_callback);
    void remove(pkey_t key, pvalue_t value, int ttl, const std::string &secret, 
		mutate_callback_t remove_callback);
    void wait();

private:
    typedef cache_mt<pkey_t, pvalue_t, lock_t, HashFcn, cache_mt_LRU<pkey_t, HashFcn> > cache_t;
    DHT dht;
    cache_t cache, probe_cache;
    
    void get_callback(pkey_t key, get_callback_t getvalue_callback, pvalue_t result);
};

template<class DHT, class HashFcn>
inline void cached_dht<DHT, HashFcn>::wait() {
    dht.wait();
}

template<class DHT, class HashFcn>
inline cached_dht<DHT, HashFcn>::cached_dht(boost::asio::io_service &io_service, 
					    unsigned int r, unsigned int t, unsigned int size) 
    : dht(io_service, r, t), cache(size), probe_cache(size) { }

template<class DHT, class HashFcn>
inline cached_dht<DHT, HashFcn>::~cached_dht() { }

template<class DHT, class HashFcn>
inline void cached_dht<DHT, HashFcn>::addGateway(const std::string &host, const std::string &service) {
    dht.addGateway(host, service);
}

template<class DHT, class HashFcn>
void cached_dht<DHT, HashFcn>::put(pkey_t key, pvalue_t value, int ttl, const std::string &secret, mutate_callback_t put_callback) {    
    cache.write(key, value);
    dht.put(key, value, ttl, secret, put_callback);
}

template<class DHT, class HashFcn>
void cached_dht<DHT, HashFcn>::remove(pkey_t key, pvalue_t value, int ttl, const std::string &secret, mutate_callback_t remove_callback) {
    cache.free(key);
    dht.remove(key, value, ttl, secret, remove_callback);
}

template<class DHT, class HashFcn>
void cached_dht<DHT, HashFcn>::get_callback(pkey_t key, get_callback_t getvalue_callback, pvalue_t result) {
    cache.write(key, result);
    getvalue_callback(result);
}

template<class DHT, class HashFcn>
void cached_dht<DHT, HashFcn>::get(pkey_t key, get_callback_t getvalue_callback) {
    pvalue_t result;
    if (cache.read(key, &result))
	getvalue_callback(result);
    else
	dht.get(key, boost::bind(&cached_dht<DHT, HashFcn>::get_callback, this, 
				 key, getvalue_callback, _1));
}

template<class DHT, class HashFcn>
void cached_dht<DHT, HashFcn>::probe(pkey_t key, get_callback_t getvalue_callback) {
    pvalue_t result;
    if (cache.read(key, &result))
	getvalue_callback(result);
    else
	dht.probe(key, boost::bind(&cached_dht<DHT, HashFcn>::get_callback, this, 
				   key, getvalue_callback, _1));
}

#endif
