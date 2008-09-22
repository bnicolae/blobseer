#ifndef __LOCKMGR
#define __LOCKMGR

const int32_t LOCKMGR_GETTICKET = 1;
const int32_t LOCKMGR_CREATE = 2;
const int32_t LOCKMGR_LASTVER = 3;
const int32_t LOCKMGR_PUBLISH = 4;
const int32_t LOCKMGR_GETRANGEVER = 5;
const int32_t LOCKMGR_GETOBJNO = 6;

const int32_t LOCKMGR_VERSION = 0;

const int32_t LOCKMGR_OK = 0;
const int32_t LOCKMGR_EARG = -1;
const int32_t LOCKMGR_EEXISTING = -2;

#endif
