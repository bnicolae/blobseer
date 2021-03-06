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

#ifndef __MIGRATION_WRAPPER
#define __MIGRATION_WRAPPER

#include "client/object_handler.hpp" 

#define MIGR_READ       0x1
#define MIGR_TRANSFER   0x2
#define MIGR_PUSH       0x3

class migration_wrapper : public object_handler {
public:
    typedef boost::function<bool (boost::uint64_t)> updater_t;

    migration_wrapper(std::string &cfg_file) : object_handler(cfg_file) { }
    ~migration_wrapper() { }
	
    bool read(boost::uint64_t offset, boost::uint64_t size, char *buffer, 
	      boost::uint32_t version = 0, boost::uint32_t threshold = 0xFFFFFFFF,
	      const blob::prefetch_list_t &prefetch_list = blob::prefetch_list_t());
    void migr_exec(updater_t updater, updater_t cancel_chunk, const std::string &service);
    void start(const std::string &host,
	       const std::string &service,
	       const std::vector<bool> &written_map);
    bool transfer_control();
    void terminate();
    bool push_chunk(unsigned int idx, char *content);
    void assign_mapping(char *mapping_) {
	mapping = mapping_;
    }

    void touch(unsigned int idx);
    void untouch(unsigned int idx);
    int next_chunk_to_push();

private:
    typedef std::set<unsigned int> migr_set_t;

    char *mapping;
    boost::uint32_t chunks_left;
    std::string source_host, source_port, target_host, target_port;
    boost::asio::io_service *io_service;
    
    migr_set_t untouched, touched;
    boost::mutex touch_lock, rpc_client_lock;

    rpcreturn_t read_chunk(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t accept_chunk(updater_t cancel_chunk,
			     const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t receive_control(updater_t updater, const rpcvector_t &params, 
				rpcvector_t &result, const std::string &id);
};

#endif
