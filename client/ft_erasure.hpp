#ifndef __FT_ERASURE
#define __FT_ERASURE

#include <boost/dynamic_bitset.hpp>

#include "ft_engine.hpp"

class ft_erasure_t : public ft_engine_t {
public:
    ft_erasure_t(rpc_client_t *rc, unsigned int retries) : ft_engine_t(rc, retries),
							   data(NULL), coding(NULL) { }
    ~ft_erasure_t();
    
    unsigned int get_groups(unsigned int no_chunks);
    unsigned int get_group_size(unsigned int no_chunks);
    void set_params(boost::uint64_t cs, unsigned int fti);

    bool write_chunks(const metadata::query_t &range, metadata::provider_list_t &adv,
		      std::vector<buffer_wrapper> &keys, 
		      std::vector<buffer_wrapper> &contents);
    void enqueue_read_chunk(random_select &policy, boost::uint64_t offset, 
			    buffer_wrapper where, bool &result);

private:
    char **data, **coding;

    void rpc_write_callback(boost::dynamic_bitset<> &res, 
			    const metadata::provider_desc &adv,
			    buffer_wrapper key, buffer_wrapper value,
			    unsigned int k, unsigned int ret_no,
			    const rpcreturn_t &error, const rpcvector_t &);

    void rpc_read_callback(boost::int32_t call_type, bool &result,
			   rpcvector_t read_params,
			   metadata::provider_desc &adv, unsigned int ret_no,
			   buffer_wrapper buffer,
			   const rpcreturn_t &error, const rpcvector_t &val);
};

#endif
