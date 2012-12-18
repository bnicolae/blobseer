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

#ifndef __PMGR_LISTENER
#define __PMGR_LISTENER

#include <vector>

#include "common/config.hpp"
#include "rpc/rpc_client.hpp"
#include "provider/provider_adv.hpp"
#include "pmanager/publisher.hpp"
#include "provider/provider.hpp"
#include "provider/page_manager.hpp"

class pmgr_listener {
    typedef rpc_client<config::socket_namespace> rpc_client_t;

    static const unsigned int UPDATE_TIMEOUT = 30;

    std::string phost, pservice, service;
    boost::uint64_t free_space, nr_read_pages, total_read_size;
    rpc_client_t publisher;
    boost::asio::deadline_timer timeout_timer;

    void provider_callback(const rpcreturn_t &error, const rpcvector_t &answer);
    void timeout_callback(const boost::system::error_code& error);

public:
    pmgr_listener(boost::asio::io_service &io_service, 
		  const std::string &ph, const std::string &ps,
		  const boost::uint64_t fs,
		  const std::string &service);
    void update_event(const boost::int32_t name, monitored_params_t &params);
    ~pmgr_listener();
};

#endif
