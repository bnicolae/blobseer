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

#include "ft_replication.hpp"

#include "provider/provider.hpp"

#include "common/debug.hpp"

unsigned int ft_replication_t::get_groups(unsigned int no_chunks) {
    return no_chunks;
}

unsigned int ft_replication_t::get_group_size(unsigned int no_chunks) {
    return ft_info;
}

void ft_replication_t::rpc_read_callback(boost::int32_t call_type, bool &result,
					 rpcvector_t read_params,
					 random_select &policy, unsigned int ret_no,
					 buffer_wrapper buffer,
					 const rpcreturn_t &error, const rpcvector_t &val) {
    if (error == rpcstatus::ok && val.size() == 1)
        return;
    
    metadata::provider_desc adv = policy.try_next();
    if (adv.empty()) {
        adv = policy.try_again();
        ret_no++;
    }
    if (ret_no == retries) {
        INFO("could not fetch page: " << read_params[0] << ", error is " << error 
             << "; no more replicas and/or retries left - ABORTED");
        result = false;
        return;
    }
    INFO("could not fetch page: " << read_params[0] << ", error is " << error 
         << "; next try is from: " << adv);
    direct_rpc->dispatch(adv.host, adv.service, call_type, read_params,
                         boost::bind(&ft_replication_t::rpc_read_callback, this, 
                                     call_type, boost::ref(result), read_params,
                                     boost::ref(policy), ret_no, buffer, _1, _2),
                         rpcvector_t(1, buffer));
}

void ft_replication_t::enqueue_read_chunk(random_select &policy, boost::uint64_t offset, 
					  buffer_wrapper where, bool &result) {
    rpcvector_t read_params;

    metadata::provider_desc adv = policy.try_next();
    if (adv.empty()) {
	result = false;
	return;
    }

    read_params.push_back(policy.get_key());
    if (where.size() != chunk_size) {	
	read_params.push_back(buffer_wrapper(offset, true));
	read_params.push_back(buffer_wrapper(where.size(), true));

	direct_rpc->dispatch(adv.host, adv.service, PROVIDER_READ_PARTIAL, read_params,
			     boost::bind(&ft_replication_t::rpc_read_callback, this, 
					 PROVIDER_READ_PARTIAL, boost::ref(result),
					 read_params, boost::ref(policy), 0, where, 
					 _1, _2), 
			     rpcvector_t(1, where));	
    } else
	direct_rpc->dispatch(adv.host, adv.service, PROVIDER_READ, read_params,
			     boost::bind(&ft_replication_t::rpc_read_callback, this, 
					 PROVIDER_READ, boost::ref(result),
					 read_params, boost::ref(policy), 0, where, 
					 _1, _2), 
			     rpcvector_t(1, where));	
}

void ft_replication_t::rpc_write_callback(boost::dynamic_bitset<> &res, 
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
			 boost::bind(&ft_replication_t::rpc_write_callback, this,
				     boost::ref(res), boost::cref(adv),
				     key, value, k, ret_no + 1, _1, _2));
}

bool ft_replication_t::write_chunks(const metadata::query_t &range, 
				    metadata::provider_list_t &adv,
				    std::vector<buffer_wrapper> &keys, 
				    std::vector<buffer_wrapper> &contents) {
    ASSERT(adv.size() == get_groups(keys.size()) * get_group_size(keys.size()));
    bool result = true;
    rpcvector_t write_params;
    boost::dynamic_bitset<> chunk_results(adv.size());

    TIMER_START(providers_timer);
    for (unsigned int i = 0; i < keys.size(); i++) {
	write_params.clear();
	write_params.push_back(keys[i]);
	write_params.push_back(contents[i]);
	
	for (unsigned int j = i * ft_info; j < (i + 1) * ft_info; j++) {
	    direct_rpc->dispatch(adv[j].host, adv[j].service, PROVIDER_WRITE,
				 write_params,		     
				 boost::bind(&ft_replication_t::rpc_write_callback, this,
					     boost::ref(chunk_results), boost::cref(adv[j]),
					     write_params[0], write_params[1], j, 0, 
					     _1, _2));
	}
    }
    direct_rpc->run();
 
    // make sure each page has at least one successfully written replica
    for (unsigned int i = 0; i < adv.size() && result; i += ft_info) {
	unsigned int k;
	for (k = i; k < i + ft_info; k++)
	    if (chunk_results[k])
		break;
	if (k == i + ft_info) {
	    ERROR("WRITE " << range << ": none of the replicas of chunk " << i / ft_info 
		  << " could be written successfully, aborted");
	    result = false;
	    break;
	}
    }
	
    TIMER_STOP(providers_timer, "WRITE " << range 
	       << ": Data written to providers, result: " << result);
    return result;
}
