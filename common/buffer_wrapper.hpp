#ifndef __BUFFER_WRAPPER
#define __BUFFER_WRAPPER

#include <boost/shared_array.hpp>
#include <boost/crc.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <sstream>

#include <iostream>
#include <stdexcept>

#include "common/debug.hpp"

/// Wrapper for memory buffers
/**
   This class is intended to be used like smart pointers but
   for memory buffers. A "smart" memory buffer serializes and
   reconstructs data from and to the buffer automatically.
   It relies on smart pointers and boost serialization itself.
*/
class buffer_wrapper {
public:
    friend std::ostream &operator<<(std::ostream &output, const buffer_wrapper &buf) {
	output << "(Size = '" << buf.len << "', Data = '";
	char *content = buf.get();
	for (unsigned int i = 0; i < buf.len - 1; i++)
	    output << std::hex << (unsigned int)content[i] << " ";
	output << std::hex << (unsigned int)content[buf.len - 1];
	output << std::dec << "')";
	return output;
    }

    bool operator==(const buffer_wrapper &second) const {
	if (len != second.len)
	    return false;
	return memcmp(content_ptr, second.content_ptr, len) == 0;
    }

    unsigned int size() const {
	return len;
    }

    char *get() const {
	return content_ptr;
    }

    bool empty() const {
	return len > 0 && content_ptr != NULL;
    }

    template <class T> bool getValue(T *val, bool serialize) const;

    template <class T> buffer_wrapper(T const &content, bool serialize);

    buffer_wrapper(char *ct, unsigned int size, bool is_managed = false) : 
	len(size), content_ptr(ct), hash(0) {
	if (is_managed)	    
	    content = boost::shared_array<char>();
	else
	    content = boost::shared_array<char>(ct);
    }

    buffer_wrapper(buffer_wrapper const &copy) : 
	len(copy.len), content(copy.content), content_ptr(copy.content_ptr), hash(copy.hash) { }

    buffer_wrapper() : 
	len(0), content(boost::shared_array<char>()), content_ptr(NULL), hash(0) { }

private:
    unsigned int len;
    boost::shared_array<char> content;
    char *content_ptr;
    mutable size_t hash;
    
    friend class buffer_wrapper_hash;
};

template <class T> buffer_wrapper::buffer_wrapper(T const &ct, bool serialize) : hash(0) {
    if (serialize) {
	std::stringstream buff_stream(std::ios_base::binary | std::ios_base::out);
	boost::archive::binary_oarchive ar(buff_stream);
	ar << ct;
	std::string value_representation = buff_stream.str();
	len = value_representation.size();
	content_ptr = new char[len];
	memcpy(content_ptr, value_representation.data(), len);
    } else {
	len = sizeof(T);
	content_ptr = new char[len];
	memcpy(content_ptr, &ct, len);
    }
    content = boost::shared_array<char>(content_ptr);
}

template <class T> bool buffer_wrapper::getValue(T *val, bool serialize) const {
    if (len == 0)
	throw std::runtime_error("Trying to get value from empty buffer");
    if (!serialize) {
	if (sizeof(T) != len)
	    return false;
	*val = *((T *)content_ptr);
	return true;
    } else {
	try {
	    std::stringstream buff_stream(std::string(content_ptr, len), std::ios_base::binary | std::ios_base::in);
	    boost::archive::binary_iarchive ar(buff_stream);
	    ar >> *val;
	} catch (std::exception &e) {
	    ERROR("deserialization failed: " << e.what() << ", content: " << *this);
	    return false;
	}
	return true;
    }
}

/// Hashing support for buffer wrappers, to be passed to templates
class buffer_wrapper_hash {
    typedef boost::crc_optimal<32, 0x1021, 0xFFFF, 0, false, false> hash_type;
public:    
    size_t operator()(const buffer_wrapper &arg) const {
	if (!arg.hash) {
	    hash_type crc_ccitt;
	    crc_ccitt.process_bytes(arg.get(), arg.size());
	    arg.hash = crc_ccitt.checksum();
	}
	return arg.hash;
    }
};

#endif
