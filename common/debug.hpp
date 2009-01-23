/* 
    ** DEBUGGING CONFIGURATION **
    - INFO > ERROR, controlled externally (i.e from the Makefile)
    - DEBUG is controlled locally. Define __DEBUG before including 'debug.hpp'.
*/

#ifndef __DEBUG_CONFIG
#define __DEBUG_CONFIG

#include <boost/date_time/posix_time/posix_time.hpp>
#ifdef __BENCHMARK
#define TIMER_START(timer) boost::posix_time::ptime timer(boost::posix_time::microsec_clock::local_time());
#define TIMER_STOP(timer, message) {\
    boost::posix_time::time_duration t = boost::posix_time::microsec_clock::local_time() - timer;\
    std::cout << "[BENCHMARK duration: " << t << " us] [" << __FILE__ << ":" << __LINE__ << ":" << __FUNCTION__ << "] " << message << std::endl; \
    }
#else
#define TIMER_START(timer) boost::posix_time::ptime timer;
#define TIMER_STOP(timer, message)
#endif

#define MESSAGE(out, level, message) \
    out << "[" << level << " " << boost::posix_time::microsec_clock::local_time() << "] [" \
    << __FILE__ << ":" << __LINE__ << ":" << __FUNCTION__ << "] " << message << std::endl

#ifdef __INFO
#define __ERROR
#define INFO(message) MESSAGE(std::cout, "INFO", message)
#else
#define INFO(message) 
#endif

#ifdef __ERROR
#define ERROR(message) MESSAGE(std::cerr, "ERROR", message)
#else
#define ERROR(message)
#endif

#endif

#undef DBG
#ifdef __DEBUG
#define DBG(message) MESSAGE(std::cout, "DEBUG", message)
#undef __DEBUG
#else
#define DBG(message)
#endif
