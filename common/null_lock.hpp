#ifndef __NULL_LOCK
#define __NULL_LOCK

/**
   Null lock implementation. 
   To be used in single threaded environments.
 */
class null_lock {
public:
    class scoped_lock {
    public:
	scoped_lock(const null_lock & /*null_param*/) { }
    };
};

#endif
