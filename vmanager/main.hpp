#ifndef __VMGR
#define __VMGR

#include <boost/cstdint.hpp>

const boost::int32_t VMGR_GETTICKET = 1;
const boost::int32_t VMGR_CREATE = 2;
const boost::int32_t VMGR_LASTVER = 3;
const boost::int32_t VMGR_PUBLISH = 4;
const boost::int32_t VMGR_GETRANGEVER = 5;
const boost::int32_t VMGR_GETOBJNO = 6;

const boost::int32_t VMGR_VERSION = 0;

const boost::int32_t VMGR_OK = 0;
const boost::int32_t VMGR_EARG = -1;
const boost::int32_t VMGR_EEXISTING = -2;

#endif
