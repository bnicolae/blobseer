#ifndef __NULL_LOCK
#define __NULL_LOCK

/// Null scoped lock
/**
   This scoped lock implementation is simply a stub. To be used with other templates
   in order to elliminate overhead when locking is of no concern.
 */
class null_lock {
public:
    class scoped_lock {
    public:
	scoped_lock(const null_lock & /*null_param*/) { }
    };
};

#endif
