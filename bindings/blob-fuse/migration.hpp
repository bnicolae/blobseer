#ifndef __MIGRATION_WRAPPER
#define __MIGRATION_WRAPPER

#include "client/object_handler.hpp" 

#define MIGR_READ       0x1
#define MIGR_START      0x2

class migration_wrapper : public object_handler {
public:
    typedef boost::function<void (boost::uint64_t)> updater_t;

    migration_wrapper(std::string &cfg_file) : object_handler(cfg_file) { }
    ~migration_wrapper() { }
	
    bool read(boost::uint64_t offset, boost::uint64_t size, char *buffer, 
	      boost::uint32_t version = 0, boost::uint32_t threshold = 0xFFFFFFFF,
	      const blob::prefetch_list_t &prefetch_list = blob::prefetch_list_t());
    void migr_exec(updater_t updater, const std::string &service);
    bool start(const std::string &host,
	       const std::string &service,
	       char *mapping,
	       const std::vector<bool> &written_map);
    void terminate();

private:
    char *mapping;
    unsigned int chunks_left;
    std::vector<bool> remote_map;
    std::string orig_host, orig_port;
    boost::asio::io_service io_service;

    rpcreturn_t read_chunk(const rpcvector_t &params, rpcvector_t &result);
    rpcreturn_t accept_start(updater_t updater, const rpcvector_t &params, 
			     rpcvector_t &result, const std::string &id);
};

#endif
