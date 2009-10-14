#ifndef __COMMON_CONFIG
#define __COMMON_CONFIG

/* 
   This file includes all the commonly used headers throughout Blob.

   All code must be thread safe. Critical sections must be protected by scoped locks.
   config::lock_t will do the trick. Here we define real mutexes only when the
   underlying infrastructure is thread based, otherwise we stick to stubs.

   For now callbacks are executed in the same thread, given multicore architectures 
   are standard, it might be a good idea to use thread pool, so you are strongly encouraged
   to use scoped locks :). 

   Finally here we define the preferred transport layer. For now we use TCP. 
   Not really likely to change, maybe future work will consider using SSL.
*/

// common boost include files
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/filesystem.hpp>

// common project include files
#include "common/cache_mt.hpp"
#include "common/buffer_wrapper.hpp"
#include "common/cached_resolver.hpp"

// ** TRANSPORT LAYER CONFIGURATION **
// transport layer: define SOCK_TYPE at compile time; valid is: TCP, UDP, maybe SSL?

namespace config {
    typedef boost::asio::ip::SOCK_TYPE socket_namespace;
}

#endif
