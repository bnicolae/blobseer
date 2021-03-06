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

#include "pmgr_listener.hpp"
#include "common/debug.hpp"

pmgr_listener::pmgr_listener(boost::asio::io_service &io_service,
			     const std::string &ph, const std::string &ps,
			     const boost::uint64_t fs,
			     const std::string &s)
    : phost(ph), pservice(ps),
      service(s),
      free_space(fs),
      nr_read_pages(0), total_read_size(0),
      publisher(io_service), 
      timeout_timer(io_service)
{
    timeout_callback(boost::system::error_code());
}

pmgr_listener::~pmgr_listener() {        
}

void pmgr_listener::update_event(const boost::int32_t name, monitored_params_t &params) {
    switch (name) {    
    case PROVIDER_WRITE:
	free_space = params.get<0>();
	if (free_space == 0)
	    timeout_callback(boost::system::error_code());
	INFO("write_page initiated by " << params.get<3>() << ", page size is: {" 
	     << params.get<2>().size() << "} (WPS)");
	INFO("free space has changed, now is: {" << params.get<0>() << "} (FSC)");
	break;
    case PROVIDER_READ:
	nr_read_pages++;
	total_read_size += params.get<2>().size();
	INFO("read_page initiated by " << params.get<3>() 
	     << ", page size is: {" << params.get<2>().size() << "} (RPS)");
	break;
    default:
	ERROR("Unknown hook type: " << name);
    }
}

void pmgr_listener::timeout_callback(const boost::system::error_code& error) {
    if (error != boost::asio::error::operation_aborted) {
	rpcvector_t params;

	params.push_back(buffer_wrapper(free_space, true));
	params.push_back(buffer_wrapper(nr_read_pages, true));
	params.push_back(buffer_wrapper(total_read_size, true));
	params.push_back(buffer_wrapper(service, true));
	publisher.dispatch(phost, pservice, PUBLISHER_UPDATE, params,
			   boost::bind(&pmgr_listener::provider_callback, this, _1, _2));
    }
}

void pmgr_listener::provider_callback(const rpcreturn_t &error, const rpcvector_t &/*buff_result*/) {
    if (error != rpcstatus::ok)
	INFO("update callback failure, retrying in " << UPDATE_TIMEOUT << "s");
    nr_read_pages = 0;
    total_read_size = 0;
    timeout_timer.expires_from_now(boost::posix_time::seconds(UPDATE_TIMEOUT));
    timeout_timer.async_wait(boost::bind(&pmgr_listener::timeout_callback, this, _1));
}
