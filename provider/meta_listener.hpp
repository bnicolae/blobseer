#ifndef __META_LISTENER
#define __META_LISTENER

#include "common/structures.hpp"
#include "provider/page_manager.hpp"

class meta_listener {
public:
    meta_listener();
    void update_event(const boost::int32_t name, monitored_params_t &params);
    ~meta_listener();
};

#endif
