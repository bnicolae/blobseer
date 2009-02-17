#ifndef __PROVIDER
#define __PROVIDER

#include <boost/cstdint.hpp>

const boost::int32_t PROVIDER_READ = 1;
const boost::int32_t PROVIDER_WRITE = 2;
const boost::int32_t PROVIDER_FREE = 3;
const boost::int32_t PROVIDER_VERSION = 0;

const boost::int32_t PROVIDER_OK = 0;
const boost::int32_t PROVIDER_EARG = -1;
const boost::int32_t PROVIDER_EMEM = -2;
const boost::int32_t PROVIDER_ENOTFOUND = -3;

#endif
