#ifndef __PROVIDER_ADV
#define __PROVIDER_ADV

#include <iostream>
#include <boost/functional/hash.hpp>

class provider_adv {
    std::string host, service;
    uint32_t free, update_rate;
    mutable size_t hash;

    friend class provider_adv_hash;
public:
    provider_adv() : host(""), service(""), free(0), update_rate(0), hash(0) { }
    provider_adv(const std::string &h, const std::string &s, unsigned int f = 0, unsigned int u = 0) :
      host(h), service(s), free(f), update_rate(u), hash(0) { }

    friend std::ostream &operator<<(std::ostream &out, const provider_adv &adv) {
	out << "(" << adv.host << ":" << adv.service << ", " << adv.free << ")";
	return out;
    }
    bool operator==(const provider_adv &adv) const {
	return host == adv.host && service == adv.service;
    }
    const std::string &get_host() const { 
	return host;
    }
    const std::string &get_service() const {
	return service;
    }
    uint32_t get_free() const {
	return free;
    }
    uint32_t get_update_rate() const {
	return update_rate;
    }
    void set_free(uint32_t new_free) {
	free = new_free;
    }
    void set_update_rate(uint32_t new_update) {
	update_rate = new_update;
    }
    void set_host(const std::string &h) {
	host = h;
    }
    bool empty() {
	return host.empty() && service.empty();
    }
    template <class Archive> void serialize(Archive &ar, unsigned int) {
	ar & host & service & free & update_rate;
    }
};

/// Hashing support for provider advertisements, to be passed to templates
class provider_adv_hash {    
    boost::hash<std::pair<std::string, std::string> > str_pair_hash;
public:    
    size_t operator()(const provider_adv &arg) const {		
	if (!arg.hash)
	    arg.hash = str_pair_hash(std::pair<std::string, std::string>(arg.host, arg.service));
	    //arg.hash = cstr_hash((arg.host + arg.service).c_str());
	return arg.hash;
    }
};

#endif
