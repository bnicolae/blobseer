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

#ifndef __PROVIDER_ADV
#define __PROVIDER_ADV

#include <iostream>
#include <boost/functional/hash.hpp>

class provider_adv {
    std::string host, service;
    boost::uint64_t free;
    boost::uint32_t update_rate;
    mutable size_t hash;

    friend class provider_adv_hash;
public:
    provider_adv() : host(""), service(""), free(0), update_rate(0), hash(0) { }

    provider_adv(const std::string &h, const std::string &s, boost::uint64_t f = 0, boost::uint32_t u = 0) :
      host(h), service(s), free(f), update_rate(u), hash(0) { }

    friend std::ostream &operator<<(std::ostream &out, const provider_adv &adv) {
	out << "(" << adv.host << ":" << adv.service << ", " << adv.free << ")";
	return out;
    }

    bool operator<(const provider_adv &adv) const {
	if (host < adv.host)
	    return true;
	if (host > adv.host)
	    return false;
	return service < adv.service;
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

    boost::uint64_t get_free() const {
	return free;
    }

    boost::uint32_t get_update_rate() const {
	return update_rate;
    }

    void set_free(boost::uint64_t new_free) {
	free = new_free;
    }

    void set_update_rate(boost::uint32_t new_update) {
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
	return arg.hash;
    }
};

#endif
