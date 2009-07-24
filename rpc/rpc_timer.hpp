#ifndef __RPC_TIMER
#define __RPC_TIMER

#include <boost/asio.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>

#define __DEBUG
#include "common/debug.hpp"

/// Thread safe per RPC timeout management
template <class Transport, class Lock>
class timer_queue_t {
public:
    typedef boost::shared_ptr<typename Transport::socket> psocket_t;

private:
    class timer_entry_t {
    public:
	class sock_entry_t {
	public:
	    psocket_t sock;
	    boost::posix_time::ptime time;
	
	    sock_entry_t(psocket_t sock_, const boost::posix_time::ptime &time_) : sock(sock_), time(time_) { }
	    bool operator<(const sock_entry_t &s) const {
		return time < s.time;
	    }
	};

	unsigned int id;
	sock_entry_t info;

	timer_entry_t(unsigned int id_, psocket_t sock_, const boost::posix_time::ptime &time_) :
	    id(id_), info(sock_entry_t(sock_, time_)) { }
    };

    typedef struct {} tid;
    typedef struct {} tinfo;
    typedef boost::multi_index_container<
	timer_entry_t,
	boost::multi_index::indexed_by <
	    boost::multi_index::hashed_unique<
		boost::multi_index::tag<tid>, BOOST_MULTI_INDEX_MEMBER(timer_entry_t, unsigned int, id)
		>,
	    boost::multi_index::ordered_non_unique<
		boost::multi_index::tag<tinfo>, 
		BOOST_MULTI_INDEX_MEMBER(timer_entry_t, typename timer_entry_t::sock_entry_t, info)
		> 
	    >
	> timer_table_t;
    
    typedef typename boost::multi_index::index<timer_table_t, tid>::type timer_table_by_id;
    typedef typename boost::multi_index::index<timer_table_t, tinfo>::type timer_table_by_info;
    typedef typename Lock::scoped_lock scoped_lock;

    Lock mutex;
    timer_table_t timer_table;
    boost::posix_time::ptime far_future_time; //far_past_time;
    boost::asio::deadline_timer timeout_timer;
    boost::thread watchdog;

    void on_timeout(const boost::system::error_code& error);
    void watchdog_exec();

public:
    timer_queue_t(boost::asio::io_service &io) : 
	far_future_time(boost::posix_time::time_from_string("10000-01-01")), 
//	far_past_time(boost::posix_time::time_from_string("1400-01-01")), 
	timeout_timer(io, far_future_time) { 
	watchdog = boost::thread(boost::bind(&timer_queue_t::watchdog_exec, this));
    }

    ~timer_queue_t() {
	watchdog.interrupt();
	watchdog.join();
    }

    void add_timer(unsigned int id, psocket_t socket, const boost::posix_time::ptime &t);
    void cancel_timer(unsigned int id);
    void clear();
};

template <class Transport, class Lock>
void timer_queue_t<Transport, Lock>::watchdog_exec() {
    const unsigned int WATCHDOG_TIMEOUT = 5;
    boost::xtime xt;

    while (1) {	
	boost::xtime_get(&xt, boost::TIME_UTC);
	xt.sec += WATCHDOG_TIMEOUT;
	boost::thread::sleep(xt);
	
	{
	    boost::this_thread::disable_interruption di;
	    scoped_lock lock(mutex);
	    timer_table_by_info &time_index = timer_table.template get<tinfo>();
	    INFO("WATCHDOG: timer timeout = " << timeout_timer.expires_at() <<  "; timer table size = " 
		 << timer_table.size() << "; timer table first entry timeout = " << time_index.begin()->info.time);
	}
    }
}

template <class Transport, class Lock>
void timer_queue_t<Transport, Lock>::add_timer(unsigned int id, psocket_t sock, const boost::posix_time::ptime &t) {
    scoped_lock lock(mutex);

    timer_table.insert(timer_entry_t(id, sock, t));
    if (timeout_timer.expires_at() > t) {
	timeout_timer.expires_at(t);
	timeout_timer.async_wait(boost::bind(&timer_queue_t<Transport, Lock>::on_timeout, this, _1));
    }
}

template <class Transport, class Lock>
void timer_queue_t<Transport, Lock>::cancel_timer(unsigned int id) {
    scoped_lock lock(mutex);

    timer_table_by_id &id_index = timer_table.template get<tid>();
    typename timer_table_by_id::iterator ai = id_index.find(id);
    if (ai != id_index.end()) {
	boost::posix_time::ptime entry_time = ai->info.time;
	id_index.erase(ai);
	if (entry_time <= timeout_timer.expires_at())
	    on_timeout(boost::system::error_code());
    }
}

template <class Transport, class Lock>
void timer_queue_t<Transport, Lock>::clear() {
    scoped_lock lock(mutex);

    timer_table.clear();
    timeout_timer.expires_at(far_future_time);
}

template <class Transport, class Lock>
void timer_queue_t<Transport, Lock>::on_timeout(const boost::system::error_code& error) {
    if (error != boost::asio::error::operation_aborted) {
	scoped_lock lock(mutex);

	boost::posix_time::ptime now = timeout_timer.expires_at();
	timer_table_by_info &time_index = timer_table.template get<tinfo>();
	for (typename timer_table_by_info::iterator ai = time_index.begin(); ai != time_index.end(); ai = time_index.begin()) {
	    timer_entry_t e = *ai;
	    if (e.info.time < now) {
		e.info.sock->cancel();
		time_index.erase(ai);
		DBG("timeout triggered by RPC id: " << e.id);
	    } else {	
		timeout_timer.expires_at(e.info.time);
		timeout_timer.async_wait(boost::bind(&timer_queue_t<Transport, Lock>::on_timeout, this, _1));
		return;
	    }
	}
	timeout_timer.expires_at(far_future_time);
    }
}

#endif
