#ifndef __COMMON_HASHERS
#define __COMMON_HASHERS

#include <ext/hash_map>

typedef std::pair<std::string, std::string> string_pair_t;

// Define the very useful string hashers
namespace __gnu_cxx {    
    template<> struct hash<std::string> {
	size_t operator()( const std::string& x ) const {
	    return hash< const char* >()( x.c_str() );
	}
    };
    template<> struct hash<string_pair_t> {
	size_t operator()( const string_pair_t& x ) const {
	    return hash<const char*>()(x.first.c_str()) ^ hash<const char*>()(x.second.c_str());
	}
    };
}

#endif
