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

#include "ft_erasure.hpp"

#include "provider/provider.hpp"

extern "C" {
#include "lib/jerasure.h"
}

#include "common/debug.hpp"

ft_erasure_t::~ft_erasure_t() {
    delete []data;
    if (coding != NULL) {
	for (unsigned int j = 0; j < ft_info; j++)
	    delete []coding[j];	
	delete []coding;
    }
    jerasure_cleanup();
}

unsigned int ft_erasure_t::get_groups(unsigned int no_chunks) {
    return no_chunks;
}

unsigned int ft_erasure_t::get_group_size(unsigned int no_chunks) {
    return 2 * ft_info;
}

void ft_erasure_t::set_params(boost::uint64_t cs, unsigned int fti) {
    delete []data;
    if (coding != NULL) {
	for (unsigned int j = 0; j < ft_info; j++)
	    delete []coding[j];
	delete []coding;
    }
    ft_engine_t::set_params(cs, fti);
    data = new char *[ft_info];
    coding = new char *[ft_info];
    for (unsigned int j = 0; j < ft_info; j++)
	coding[j] = new char[chunk_size / ft_info];
    jerasure_init(ft_info);
    DBG("initialized with chunk_size = " << chunk_size << " and ft_info = " << ft_info);
}


void ft_erasure_t::rpc_read_callback(boost::int32_t call_type, bool &result,
				     rpcvector_t read_params,
				     metadata::provider_desc &adv, unsigned int ret_no,
				     buffer_wrapper buffer,
				     const rpcreturn_t &error, const rpcvector_t &val) {
    if (error == rpcstatus::ok && val.size() == 1)
        return;

    ret_no++;
    if (ret_no == retries) {
        INFO("could not fetch shard: " << read_params[0] << ", error is " << error 
             << "; no more retries left - ABORTED");
        result = false;
        return;
    }
    INFO("could not fetch shard: " << read_params[0] << ", error is " << error 
         << "; retries left: " << retries - ret_no);
    direct_rpc->dispatch(adv.host, adv.service, call_type, read_params,
                         boost::bind(&ft_erasure_t::rpc_read_callback, this, 
                                     call_type, boost::ref(result), read_params,
                                     boost::ref(adv), ret_no, buffer, _1, _2),
                         rpcvector_t(1, buffer));
}

void ft_erasure_t::enqueue_read_chunk(random_select &policy, boost::uint64_t offset, 
				      buffer_wrapper where, bool &result) {
    if (where.size() != chunk_size)
	FATAL("partial reads for erasure codes are not implemented yet");

    metadata::provider_list_t &providers = policy.get_providers();

    if (providers.size() < ft_info) {
	ERROR("not enough shards (" << providers.size() << "/" << ft_info 
	      << ") for chunk " << policy.get_key() << ", read aborted");
	result = false;
	return;
    }

    buffer_wrapper key_clone, buffer;
    boost::uint64_t ft_shard = chunk_size / ft_info;
    rpcvector_t read_params;

    for (unsigned int j = 0; j < ft_info; j++) {
	key_clone.clone(policy.get_key());
	(key_clone.get())[key_clone.size() - 1] += j;
	read_params.clear();
	read_params.push_back(key_clone);

	buffer = buffer_wrapper(where.get() + j * ft_shard, ft_shard, true);
	direct_rpc->dispatch(providers[j].host, providers[j].service, PROVIDER_READ, read_params,
			     boost::bind(&ft_erasure_t::rpc_read_callback, this, 
					 PROVIDER_READ, boost::ref(result), read_params,
					 boost::ref(providers[j]), 0, buffer, _1, _2),
			     rpcvector_t(1, buffer));
    }
}

void ft_erasure_t::rpc_write_callback(boost::dynamic_bitset<> &res, 
					  const metadata::provider_desc &adv,
					  buffer_wrapper key, buffer_wrapper value,
					  unsigned int k, unsigned int ret_no,
					  const rpcreturn_t &error, const rpcvector_t &) {
    res[k] = (error == rpcstatus::ok);
    if (res[k])
	return;

    INFO("a replica of page " << k << " could not be written successfully, error is: " 
	 << error);
    if (ret_no == retries)
	return;

    rpcvector_t write_params;
    write_params.push_back(key);
    write_params.push_back(value);
    direct_rpc->dispatch(adv.host, adv.service, PROVIDER_WRITE, write_params,
			 boost::bind(&ft_erasure_t::rpc_write_callback, this,
				     boost::ref(res), boost::cref(adv),
				     key, value, k, ret_no + 1, _1, _2));
}

bool ft_erasure_t::write_chunks(const metadata::query_t &range,
				metadata::provider_list_t &adv,
				std::vector<buffer_wrapper> &keys, 
				std::vector<buffer_wrapper> &contents) {
    ASSERT(adv.size() == get_groups(keys.size()) * get_group_size(keys.size()));

    bool result = true;
    rpcvector_t write_params;
    boost::dynamic_bitset<> adv_results(adv.size());
    buffer_wrapper key_clone;

    TIMER_START(providers_timer);
    for (unsigned int i = 0; i < keys.size(); i++) {
	for (unsigned int j = 0; j < ft_info; j++)
	    data[j] = contents[i].get() + j * (chunk_size / ft_info);
	jerasure_encode(data, coding, chunk_size / ft_info);
	for (unsigned int j = 0; j < ft_info; j++) {
	    key_clone.clone(keys[i]);
	    (key_clone.get())[key_clone.size() - 1] += j;
	    write_params.clear();
	    write_params.push_back(key_clone);
	    write_params.push_back(buffer_wrapper(data[j], chunk_size / ft_info, true));
	    unsigned int k = i * 2 * ft_info + j;
	    direct_rpc->dispatch(adv[k].host, adv[k].service, 
				 PROVIDER_WRITE, write_params,		     
				 boost::bind(&ft_erasure_t::rpc_write_callback, this,
					     boost::ref(adv_results), boost::cref(adv[k]),
					     write_params[0], write_params[1], k, 0, _1, _2));
	}
	for (unsigned int j = 0; j < ft_info; j++) {
	    key_clone.clone(keys[i]);
	    (key_clone.get())[key_clone.size() - 1] += ft_info + j;
	    write_params.clear();
	    write_params.push_back(key_clone);
	    write_params.push_back(buffer_wrapper(coding[j], chunk_size / ft_info, true));
	    unsigned int k = (i * 2 + 1) * ft_info + j;
	    direct_rpc->dispatch(adv[k].host, adv[k].service, 
				 PROVIDER_WRITE, write_params,		     
				 boost::bind(&ft_erasure_t::rpc_write_callback, this,
					     boost::ref(adv_results), boost::cref(adv[k]),
					     write_params[0], write_params[1], k, 0, _1, _2));
	}
    }
    direct_rpc->run();
 
    // make sure each chunk and its encoding were written successfully
    for (unsigned int i = 0; i < adv.size(); i++) 
	if (!adv_results[i]) {
	    ERROR("WRITE " << range << ": part " << i % (2 * ft_info) 
		  <<" of chunk " << i / (2 * ft_info) 
		  << " could be written successfully, aborted");
	    result = false;
	    break;
	}
	
    TIMER_STOP(providers_timer, "WRITE " << range 
	       << ": Data written to providers, result: " << result);

    return result;
}
