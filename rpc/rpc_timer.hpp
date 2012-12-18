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

#ifndef __RPC_TIMER
#define __RPC_TIMER

#include <boost/asio.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>

#include "common/debug.hpp"

/// socket timeout management
template <class Socket>
class timer_queue_t {
public:
    typedef Socket psocket_t;
private:
    class timer_entry_t {
    public:
	psocket_t socket;
	boost::posix_time::ptime time;

	timer_entry_t(psocket_t sock_, const boost::posix_time::ptime &time_) :
	    socket(sock_), time(time_) { }
    };

    typedef struct {} tsocket;
    typedef struct {} ttime;
    typedef boost::multi_index_container<
	timer_entry_t,
	boost::multi_index::indexed_by <
	    boost::multi_index::ordered_unique<
		boost::multi_index::tag<tsocket>, BOOST_MULTI_INDEX_MEMBER(timer_entry_t, psocket_t, socket)
		>,
	    boost::multi_index::ordered_non_unique<
		boost::multi_index::tag<ttime>, BOOST_MULTI_INDEX_MEMBER(timer_entry_t, boost::posix_time::ptime, time)
		>
	    >
	> timer_table_t;
    
    typedef typename boost::multi_index::index<timer_table_t, tsocket>::type timer_table_by_socket;
    typedef typename boost::multi_index::index<timer_table_t, ttime>::type timer_table_by_time;
    typedef typename boost::mutex::scoped_lock scoped_lock;

    boost::mutex mutex;
    timer_table_t timer_table;
    boost::thread watchdog;

    void watchdog_exec();

public:
    timer_queue_t() : watchdog(boost::thread(boost::bind(&timer_queue_t::watchdog_exec, this))) { }

    ~timer_queue_t() {
	watchdog.interrupt();
	watchdog.join();
    }

    void add_timer(psocket_t socket, const boost::posix_time::ptime &t);
    void cancel_timer(psocket_t socket);
    void clear();
};

template <class Socket>
void timer_queue_t<Socket>::watchdog_exec() {
    const unsigned int WATCHDOG_TIMEOUT = 5;
    boost::xtime xt;
    timer_table_by_time &time_index = timer_table.template get<ttime>();

    while (1) {	
	boost::xtime_get(&xt, boost::TIME_UTC_);
	xt.sec += WATCHDOG_TIMEOUT;
	boost::thread::sleep(xt);

	boost::posix_time::ptime now(boost::posix_time::microsec_clock::local_time());	
	{
	    boost::this_thread::disable_interruption di;
	    scoped_lock lock(mutex);

	    for (typename timer_table_by_time::iterator ai = time_index.begin(); ai != time_index.end(); ai = time_index.begin()) {
		timer_entry_t e = *ai;
		if (e.time < now) {
		    std::stringstream out;
		    try {
			out << e.socket->socket().remote_endpoint().address().to_string() << 
			    ":" << e.socket->socket().remote_endpoint().port();
		    } catch (...) {
			out << "<unknown>";
		    }
		    e.socket->close();
		    time_index.erase(ai);

		    INFO("WATCHDOG: timeout triggered by connection: " << out.str() 
			 << ", aborted");
		} else
		    break;
	    }
	}
    }
}

template <class Socket>
void timer_queue_t<Socket>::add_timer(psocket_t sock, const boost::posix_time::ptime &t) {
    scoped_lock lock(mutex);

    timer_table.insert(timer_entry_t(sock, t));
}

template <class Socket>
void timer_queue_t<Socket>::cancel_timer(psocket_t sock) {
    scoped_lock lock(mutex);

    timer_table_by_socket &id_index = timer_table.template get<tsocket>();
    id_index.erase(sock);
}

template <class Socket>
void timer_queue_t<Socket>::clear() {
    scoped_lock lock(mutex);

    timer_table.clear();
}

#endif
