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

#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>

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
    // buffer wrappers are limited to grow to 1GB max
    static const boost::uint64_t MAX_SIZE = 1 << 30;

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
	return len == 0 || content_ptr == NULL;
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
	len(copy.len), content(copy.content), content_ptr(copy.content_ptr), hash(copy.hash) {
    }

    buffer_wrapper() :
	len(0), content(boost::shared_array<char>()), content_ptr(NULL), hash(0) { 
    }
    
    bool compress(char *in_content = NULL, unsigned int in_len = 0) {
	if (in_content == NULL && in_len == 0) {
	    in_content = content_ptr;
	    in_len = len;
	}
	char *compressed_ptr = new char[in_len + in_len / 16  + 64 + 3 + sizeof(boost::uint64_t)];
	unsigned char *work_mem = new unsigned char[LZO1X_MEM_COMPRESS];
	lzo_uint compressed_len = 0;
	
	bool result = (lzo1x_1_compress((unsigned char *)in_content, in_len, 
					(unsigned char *)(compressed_ptr + sizeof(boost::uint64_t)),
					&compressed_len, work_mem) == LZO_E_OK);
	if (result) {
	    content.reset(compressed_ptr);
	    content_ptr = content.get();
	    len = compressed_len + sizeof(boost::uint64_t);
	    boost::uint64_t len64 = in_len;
	    memcpy(content_ptr, &len64, sizeof(boost::uint64_t));
	} else
	    delete []compressed_ptr;
	delete []work_mem;
	return result;
    }

    bool decompress() {
	ASSERT(len >= sizeof(boost::uint64_t));
	boost::uint64_t decompressed_len;
	memcpy(&decompressed_len, content_ptr, sizeof(boost::uint64_t));
	ASSERT(decompressed_len < MAX_SIZE);

	char *decompressed_ptr = new char[decompressed_len];
	lzo_uint new_len = 0;    
	bool result = (lzo1x_decompress((unsigned char *)(content_ptr + sizeof(boost::uint64_t)), len - sizeof(boost::uint64_t), 
					(unsigned char *)decompressed_ptr, &new_len, NULL) == LZO_E_OK) 
	    && (new_len == decompressed_len);
	INFO("decompressed, result = " << result);
	if (result) {
	    content.reset(decompressed_ptr);
	    content_ptr = content.get();
	    len = new_len;
	} else
	    delete []decompressed_ptr;
	return result;
    }

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
