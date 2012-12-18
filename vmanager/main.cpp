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

#include <iostream>
#include <fstream>

#include <boost/bind.hpp>

#include "libconfig.h++"
#include "vmanagement.hpp"
#include "rpc/rpc_server.hpp"
#include "main.hpp"

using namespace std;

int main(int argc, char *argv[]) {   
    std::string host, service;

    if (argc != 2) {
	cout << "Usage: " << argv[0] << " <config_file>" << endl;
	cout << "Config file:\nvmanager: {\n\thost = <host_name>\n\tservice = <service_no>\n\tthreads = <max_threads>\n};\n";
	return 1;
    }

    libconfig::Config cfg;
    try {
	cfg.readFile(argv[1]);
	if (!(cfg.lookupValue("vmanager.service", service)
	      //&& cfg.lookupValue("vmanager.host", host)
	      //&& cfg.lookupValue("vmanager.threads", threads)
		))
	    throw libconfig::ConfigException();
    } catch(libconfig::FileIOException &e) {
	ERROR("I/O exception on config file");
	return 2;
    } catch(libconfig::ParseException &e) {
	ERROR("Parse exception (line " << e.getLine() << "): " << e.getError());
	return 3;
    } catch(...) {
	ERROR("Invalid configuration, check format (run without config file for help)");
	return 4;
    }

    boost::asio::io_service io_service;
    rpc_server<config::socket_namespace> vmgr_server(io_service);
    vmanagement vmgr;
    vmgr_server.register_rpc(VMGR_GETTICKET,
			     (rpcserver_extcallback_t)boost::bind(&vmanagement::get_ticket, 
								  boost::ref(vmgr), _1, _2, _3));
    vmgr_server.register_rpc(VMGR_CREATE,
			     (rpcserver_extcallback_t)boost::bind(&vmanagement::create, 
								  boost::ref(vmgr), _1, _2, _3));
    vmgr_server.register_rpc(VMGR_GETROOT,
			     (rpcserver_extcallback_t)boost::bind(&vmanagement::get_root, 
								  boost::ref(vmgr), _1, _2, _3));
    vmgr_server.register_rpc(VMGR_PUBLISH,
			     (rpcserver_extcallback_t)boost::bind(&vmanagement::publish, 
								  boost::ref(vmgr), _1, _2, _3));
    vmgr_server.register_rpc(VMGR_GETOBJNO,
			     (rpcserver_extcallback_t)boost::bind(&vmanagement::get_objcount, 
								  boost::ref(vmgr), _1, _2, _3));
    vmgr_server.register_rpc(VMGR_CLONE,
			     (rpcserver_extcallback_t)boost::bind(&vmanagement::clone, 
								  boost::ref(vmgr), _1, _2, _3));

    vmgr_server.start_listening(config::socket_namespace::endpoint(config::socket_namespace::v4(), 
								   atoi(service.c_str())));
    INFO("listening on " << vmgr_server.pretty_format_str());
    io_service.run();
    return 0;
}
