#ifndef __CACHE_MT
#define __CACHE_MT

#include <list>
#include <boost/unordered_map.hpp>

/// No cache policy
/**
   This simply schedules the last used entry for eviction.
   It is only intended as a fast stub policy when eviction is not a concern.
   (ex. when using the cache as a simple synchronized hash table and managing 
   the size externally, making sure eviction never happens in the first place)
*/
template <class Key, class HashFcn = boost::hash<Key> > class cache_mt_none {
public:
    /// Hits just assign the internal key to the argument
    void hit(const Key) {
    }
    /// Misses just assign the internal key to the argument
    void miss(const Key) {
    }
    /// Returns the internal key
    bool evictEntry(Key *) {
	return false;
    }
    /// Not really doing anything on clear
    void clear() {
    }
    /// Not really doing anything on invalidate
    void invalidate(Key) {
    }    
};

/// Least Recently Used Cache Policy
/**
   This acts as an example for cache policies. Basically
   it must implement a mechanism to schedule the next entry for eviction
   based on hits and misses.
   This example implements the LRU policy. We don't care about misses, only about
   hits. Last hit will move the entry to the back of the queue. First entry in
   the queue is the one scheduled for eviction.
*/
template <class Key, class HashFcn = boost::hash<Key> > class cache_mt_LRU {
    typedef typename std::list<Key> list_t;
    typedef typename std::list<Key>::iterator list_iterator_t;
    typedef typename std::list<Key>::reverse_iterator list_reverse_iterator_t;
    typedef typename boost::unordered_map<Key, list_iterator_t, HashFcn> hash_map_t;
    typedef typename boost::unordered_map<Key, list_iterator_t, HashFcn>::iterator hash_map_iterator_t;

    hash_map_t access_count;
    list_t queue;
public:  
    void hit(const Key &key);
    /// Trigger for cache miss
    /**
       Will be called by the cache implementation when not finding the entry on read.
       For LRU just do nothing.
       @param key The identifier of the cache entry
    */
    void miss(const Key) {
    }
    bool evictEntry(Key *key);
    /// Clears all the entries
    void clear() {
	access_count.clear();
	queue.clear();
    }
    void invalidate(const Key &key);
};

/// Invalidate the cache entry
/**
   When the cache implementation wants to invalidate an entry, removing it from the cache
   it will call this method. The cache policy is supposed to remove the entry from the 
   eviction scheduling.
*/
template <class Key, class HashFcn>
void cache_mt_LRU<Key, HashFcn>::invalidate(const Key &key) {	
    hash_map_iterator_t i = access_count.find(key);
    if (i != access_count.end()) {
	queue.erase(i->second);
	access_count.erase(key);
    }	
}

/// Trigger for cache hit
/**
   Will be called by the cache implementation on write or when finding the entry on read.
   For LRU just move the entry to the back of the queue.
   @param key The identifier of the cache entry
*/
template <class Key, class HashFcn>
void cache_mt_LRU<Key, HashFcn>::hit(const Key &key) {
    invalidate(key);
    queue.push_back(key);	    
    list_reverse_iterator_t pos = queue.rbegin();	    
    std::pair<Key, list_iterator_t> new_entry(key, (++pos).base());
    access_count.insert(new_entry);
}

/// Returns an entry to be evicted
/**
   Will suppose the implementation really evicts the page.
   @return The key of the object to be evicted out of the cache 
*/
template <class Key, class HashFcn>
bool cache_mt_LRU<Key, HashFcn>::evictEntry(Key *key) {
    if (queue.empty())
	return false;
    *key = queue.front();
    invalidate(*key);    
    return true;
}

/// Thread safe cache implementation
/**
  Manages entries identified by a key in a local cache.  
  'Key' is the type of the identifier of cache entries.
  'Value' is type of the entries to be stored.
  'Policy' is a cache policy as described by the mt_cache_LRU policy example. (Default = mt_cache_LRU)
  'Lock' is a scoped lock mutex implementation (may be used with a stub if not used in a multi-threaded environment)
  'HashFcn' is the hasher for Key, as described by the STL standard.  (Default = standard STL hasher)
*/
template <class Key, class Value, class Lock, class HashFcn = boost::hash<Key>, class Policy = cache_mt_LRU<Key, HashFcn> > 
class cache_mt {
private:
    typedef typename Lock::scoped_lock scoped_lock;
    typedef std::pair<Value, bool> entry_t;
    typedef typename boost::unordered_map<Key, entry_t, HashFcn> hash_map_t;

public:
    typedef boost::function<void (const Key &k, const Value &v)>  evict_callback_t;
    typedef typename hash_map_t::const_iterator const_iterator;

private:
    Policy policy;
    Lock hash_lock;
    hash_map_t cache_map;
    unsigned int m_size;
    evict_callback_t evict_callback;

    static void void_evict_callback(const Key &k, const Value &v) { }
    
public:
    /// Constructor
    /**
       Initializes a cache with an initial maximum size.
       @param m_size The maximum cache size, by default 1024 * 1024 entries.
    */
    cache_mt(unsigned int m = 1 << 20, 
	     evict_callback_t evict_cb = boost::bind(&void_evict_callback, _1, _2)
	) : m_size(m), evict_callback(evict_cb) { }
    /// Get cache capacity
    /** 
	Get the total number of slots in the cache.	
    */
    unsigned int max_size() const {
	return m_size;
    }
    /// Get cache size
    /** 
	Get the number of entries stored in the cache.
	This is basically useful to check if the cache is full.
    */
    unsigned int size() const {
	return cache_map.size();
    }
    /// Flush the cache
    /**
       Will clear all entries and notify the policy to do the same
    */       
    void clear() {
	scoped_lock lock(hash_lock);

	cache_map.clear();
	policy.clear();
    }

    /// Returns begin iterator 
    /**
       Returns a const iterator over the cache elements pointing at the beginning
     */
    const_iterator begin() {
	return cache_map.begin();
    }
    /// Returns end iterator 
    /**
       Returns a const iterator over the cache elements pointing at the end
     */
    const_iterator end() {
	return cache_map.end();
    }
	
    bool resize(unsigned int m_size);
    bool write(const Key &key, const Value &data, const bool dirty = true);
    bool find(const Key &key);
    bool read(const Key &key, Value *data);
    void free(const Key &key);    
};

/// Reset the maximum cache size
/**
   Will eventually evict pages from the cache to match the new maximum size
*/
template <class Key, class Value, class Lock, class HashFcn, class Policy>
bool cache_mt<Key, Value, Lock, HashFcn, Policy>::resize(unsigned int msize) {
    scoped_lock lock(hash_lock);

    if (m_size < msize) {
	m_size = msize;
	return true;
    }

    while (cache_map.size() > m_size) {
	Key k; entry_t e;
	if (!policy.evictEntry(&k))
	    return false;
	e = cache_map[k];
	cache_map.erase(k);
	if (e.second) {
	    lock.unlock();
	    evict_callback(k, e.first);
	    lock.lock();
	}
    }
    return true;
}

/// Write the entry in the cache
/**
   Writes the specified entry in the cache.
   Evicts another entry if necessary.
   Generates a hit for the cache policy.
   @param key The identifier of the entry
   @param data The entry itself
   @returns 'true' if data was stored in cache, 'false' otherwise (i.e. no eviction possible)
*/
template <class Key, class Value, class Lock, class HashFcn, class Policy>
bool cache_mt<Key, Value, Lock, HashFcn, Policy>::write(const Key &key, const Value &data, const bool dirty)  {
    Key ek; entry_t ev;

    {
	scoped_lock lock(hash_lock);

	cache_map.erase(key);
	if (cache_map.size() == m_size) {
	    if (!policy.evictEntry(&ek))
		return false;
	    ev = cache_map[ek];	
	    cache_map.erase(ek);
	}
	std::pair<Key, entry_t> new_entry(key, entry_t(data, dirty));
	cache_map.insert(new_entry);	    
	policy.hit(key);
    }
    
    if (ev.second)
	evict_callback(ek, ev.first);
    return true;
}

/// Read an entry from the cache
/**
   Generates a hit returning the requested entry or a miss returning nothing.   
   @param key The identifier of the entry
   @param data The entry where to store the result; will not be modified if data not found in cache
   @returns 'true' if 'data' was set successfully, 'false' otherwise (i.e not in cache).
*/
template <class Key, class Value, class Lock, class HashFcn, class Policy>
bool cache_mt<Key, Value, Lock, HashFcn, Policy>::read(const Key &key, Value *data) {
    scoped_lock lock(hash_lock);

    const_iterator i = cache_map.find(key);
    if (i == cache_map.end()) {
	policy.miss(key);
	return false;
    } else {
	policy.hit(key);
	*data = i->second.first;
	return true;
    }
}

template <class Key, class Value, class Lock, class HashFcn, class Policy>
bool cache_mt<Key, Value, Lock, HashFcn, Policy>::find(const Key &key) {
    scoped_lock lock(hash_lock);

    const_iterator i = cache_map.find(key);
    return i != cache_map.end();
}

/// Frees an entry from the cache.
/**
   Will invalidate the entry, notifying the policy scheduler about
   the deviation from the standard policy.
   @param key The identifier of the entry   
*/
template <class Key, class Value, class Lock, class HashFcn, class Policy>
void cache_mt<Key, Value, Lock, HashFcn, Policy>::free(const Key &key) {
    scoped_lock lock(hash_lock);

    policy.invalidate(key);
    cache_map.erase(key);
}

#endif
