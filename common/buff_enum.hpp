#ifndef __BUFFERED_ENUMERATION
#define __BUFFERED_ENUMERATION

#include <vector>
#include <boost/shared_ptr.hpp>

/// Abstract buffered enumeration
/** 
    Provides the basic enumeration interface (hasNext, getNext) but
    allows hasNext() to prefetch up to max N next entries (default 1)
    if the local buffer is empty. 
*/

template <class T> class buffered_enumeration {
protected:
    unsigned int last_index;
    std::vector<boost::shared_ptr<T> > buffer;
public:
    /// Get up to max no next entries in the local enumeration buffer
    /** 
	This method will be implemented by actual BufferedEnumerations.
	The implementation will put the results in the
	@param max The maximal number of next items to fetch in the local buffer
	@return The actual number of next items filled in the buffer
    */
    virtual int fetchNext(int max) = 0;
    /// Constructor
    /**
       Sets the index to the beginning of the local buffer, the inheriting class'
       constructor is responsible for the initial buffer fill.
    */
    buffered_enumeration() {
	last_index = 0;
    }
    /// Destructor
    /** For now we do not have anything to destroy */
    virtual ~buffered_enumeration() { }
    /// Return the next item in the enumeration
    /**              
       @return Smart pointer to the next item in the enumeration.
    */
    boost::shared_ptr<T> getNext() {
	return buffer[last_index++];
    }
    /// Do we have more items in the enumeration or not?
    /**
       If there are more items in the local buffer return true, otherwise false
       @param max The maximal number of next items to fetch in the local buffer
       @return true if there is a next item, false otherwise
    */
    bool hasNext() {
	return last_index < buffer.size();
    }
};

#endif
