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

#ifndef __ASYNC_DHT
#define __ASYNC_DHT

#include <boost/function.hpp>
#include <boost/threadpool.hpp>

#include "bamboo_dht.hpp"

template<class DHT>
class async_dht {
public:      
    typedef typename DHT::pkey_type pkey_type;
    typedef typename DHT::pvalue_type pvalue_type;
    // TO DO: throw exceptions form DHT, capture them and modify to "void (const boost::system::error_code &, int)"
    typedef boost::function<void (int)> mutate_callback_type;
    typedef boost::function<void (pvalue_type)> get_callback_type;

    /*
    typedef boost::function<void (enumeration_type)> getall_callback_type;
    typedef typename DHT::enumeration_type enumeration_type;
    */

    async_dht(int rc = 3, int to = 5, int cache_size = 0);
    ~async_dht();
    void addGateway(const std::string &host, const std::string &service);
    void put(pkey_type key, pvalue_type value, int ttl, const std::string &secret, mutate_callback_type put_callback);
    
    void get(pkey_type key, get_callback_type get_callback);

    void remove(pkey_type key, pvalue_type value, int ttl, const std::string &secret, mutate_callback_type remove_callback);
    void wait();

    /*
    void getFirst(pkey_type key, int step, getall_callback_type getall_callback);
    void getNext(enumeration_type e, int step, getall_callback_type getall_callback);
    */

private:
    typedef typename boost::threadpool::fifo_pool thread_pool_type;

    static const unsigned int MAX_THREAD_NO = 100;
    DHT *dht;
    thread_pool_type *tp;    
    
    /*
    void execGet(pkey_type key, int step, getall_callback_type getall_callback);
    static void execNext(enumeration_type e, int step, getall_callback_type get_callback);
    */
};

template<class DHT>
inline void async_dht<DHT>::wait() {
    tp->wait();
}

template<class DHT>
inline async_dht<DHT>::async_dht(int rc, int to, int /* cache_size */) {
    dht = new DHT(rc, to);
    tp = new thread_pool_type(MAX_THREAD_NO);
}

template<class DHT>
inline async_dht<DHT>::~async_dht() {
    delete dht;
    delete tp;
}

template<class DHT>
inline void async_dht<DHT>::addGateway(const std::string &host, const std::string &service) {
    dht->addGateway(host, service);
}

template<class DHT>
void async_dht<DHT>::put(pkey_type key, pvalue_type value, int ttl, const std::string &secret, mutate_callback_type put_callback) {
    tp->schedule(boost::bind(put_callback, boost::bind(&DHT::put, dht, key, value, ttl, secret)));
}

template<class DHT>
void async_dht<DHT>::remove(pkey_type key, pvalue_type value, int ttl, const std::string &secret, mutate_callback_type remove_callback) {
    tp->schedule(boost::bind(remove_callback, boost::bind(&DHT::remove, dht, key, value, ttl, secret)));
}

template<class DHT>
void async_dht<DHT>::get(pkey_type key, get_callback_type getvalue_callback) {
    tp->schedule(boost::bind(getvalue_callback, boost::bind(&DHT::getValue, dht, key)));
}

/*
template<class DHT>
void async_dht<DHT>::execGet(pkey_type key, int max, getall_callback_type get_callback) {
    execNext(dht->getEnumeration(key), max, get_callback);
}

template<class DHT>
void async_dht<DHT>::execNext(enumeration_type e, int max, getall_callback_type get_callback) {
    if (e.get() != NULL)
	e->fetchNext(max);
    get_callback(e);
}

template<class DHT>
void async_dht<DHT>::getNext(enumeration_type e, int max, getall_callback_type get_callback) {
    tp->schedule(boost::bind(execNext, e, max, get_callback));
}

template<class DHT>
void async_dht<DHT>::getFirst(pkey_type key, int step, getall_callback_type get_callback) {
    tp->schedule(boost::bind(&async_dht<DHT>::execGet, this, key, step, get_callback));
}
*/

#endif
