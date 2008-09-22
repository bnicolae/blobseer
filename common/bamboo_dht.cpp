#include "bamboo_dht.hpp"
#include "common/configuration.hpp"

bamboo_dht::bamboo_dht(int rc, int to) {    
    retry_count = rc;
    time_out = to;
}

bamboo_dht::~bamboo_dht() {
}

void bamboo_dht::gateway_callback(const boost::system::error_code &error, ) {
    if (error) {
	INFO("could not load resolve " << host << ":" << service);
	return;
    }
    gateways.push_back(addr_desc_type(host, service));
}

void bamboo_dht::addGateway(const std::string &host, const std::string &service) {
    
    try {
	gateways.push_back(addr_desc_type(host, service));
    } catch (...) {
	INFO("could not load resolve " << host << ":" << service);
    }
    DBG("added (" << host << ":" << service << ")");
}

bamboo_dht::pvalue_type bamboo_dht::getValue(pkey_type key) {
    bamboo_get_args get_args;
    CLIENT *clnt = NULL;    
    int no = 0, ret = -1, gid = -1;

    get_args.application = APP_STRING;
    get_args.client_library = CLIB_STRING;

    // set the key (SHA1 of key)
    SHA1((unsigned char *)key.get(), key.size(), (unsigned char *)get_args.key);

    get_args.placemark.bamboo_placemark_len = 0;
    get_args.placemark.bamboo_placemark_val = NULL;
    get_args.maxvals = 1;

    bamboo_get_result get_result;
    memset(&get_result, 0, sizeof(bamboo_get_result));
    while (ret != 0 && no < this->retry_count) {
	clnt = getRPCConnection(no, gid);
	if (clnt == NULL)
	    continue;
	ret = bamboo_dht_proc_get_3(&get_args, &get_result, clnt);
	if (ret != 0)
	    INFO(this->gateways[gid] << ": " << clnt_sperror(clnt, "RPC exec error, something is fishy"));
	clnt_destroy(clnt);
    }
    if (ret == 0) {
	if (get_result.values.values_len == 1) {
	    int len = get_result.values.values_val[0].value.bamboo_value_len;
	    char *val = new char[len];
	    memcpy(val, get_result.values.values_val[0].value.bamboo_value_val, len);
	    free(get_result.values.values_val[0].secret_hash.algorithm);
	    free(get_result.values.values_val[0].secret_hash.hash.hash_val);
	    free(get_result.values.values_val[0].value.bamboo_value_val);
	    free(get_result.values.values_val);
	    return pvalue_type(val, len);
	} else {
	    INFO("get for " << key << " failed, bamboo returned " << get_result.values.values_len << " values");
	    return pvalue_type();
	}
    } else {
	ERROR("get for " << key << " ultimately failed, number of retries expended");
	return pvalue_type();
    }
}

CLIENT *bamboo_dht::getRPCConnection(int &no, int &gid) {
    if (this->gateways.size() == 0)
	return NULL;

    struct timeval tv;
    CLIENT *clnt = NULL;
    int sockp = RPC_ANYSOCK;
    
    while (clnt == NULL && no < this->retry_count) {
	gid = rand() % this->gateways.size();
	// cout << "Trying (" << *no << ") gateway: " << index << endl;
	clnt = clnttcp_create(this->gateways[gid].getNative(), BAMBOO_DHT_GATEWAY_PROGRAM, BAMBOO_DHT_GATEWAY_VERSION_3, &sockp, 0, 0);
	no++;
    }
    if (clnt != NULL) {
	tv.tv_sec = time_out;
	tv.tv_usec = 0;
	clnt_control(clnt, CLSET_TIMEOUT, (char *)&tv);
    } else
	ERROR("could not establish a new connection; retires expended");

    return clnt;
}

int bamboo_dht::put(pkey_type key, pvalue_type value, int ttl, const std::string &secret) {
    int no = 0, gid = -1;

    // set the general arguments
    bamboo_put_arguments put_args;
    put_args.application = APP_STRING;
    put_args.client_library = CLIB_STRING;
    put_args.ttl_sec = ttl; 

    // if the value representation does not fit in the DHT limits, fail
    if (value.size() > VALUE_SIZE_MAX)
	return E_MAXVALUE_EXCEEDED;

    // set the key (SHA1 of key)
    SHA1((unsigned char *)key.get(), key.size(), (unsigned char *)put_args.key);

    // set the value
    put_args.value.bamboo_value_len = value.size();
    put_args.value.bamboo_value_val = (char *)value.get();

    // set the secret
    put_args.secret_hash.algorithm = HASH_DESC;
    put_args.secret_hash.hash.hash_len = HASH_LEN;

    put_args.secret_hash.hash.hash_val = (char *)malloc(HASH_LEN);
    if (secret != "")
	SHA1((unsigned char *)secret.data(), secret.size(), (unsigned char *)put_args.secret_hash.hash.hash_val);
    else
	SHA1((unsigned char *)HASH_DESC, strlen(HASH_DESC), (unsigned char *)put_args.secret_hash.hash.hash_val);

    bamboo_stat put_result = BAMBOO_AGAIN;
    int ret = 0;
    CLIENT *clnt = NULL;
    while ((ret != 0 || put_result != BAMBOO_OK) && no < this->retry_count) {
	clnt = getRPCConnection(no, gid);
	if (clnt == NULL)
	    continue;
	ret = bamboo_dht_proc_put_3(&put_args, &put_result, clnt);
	if (ret != 0)
	    INFO("Contacting " << this->gateways[gid] << ": " << clnt_sperror(clnt, "RPC exec error, something is fishy") << ", retries left: " << no);
	clnt_destroy(clnt);
    }

    free(put_args.secret_hash.hash.hash_val);

    if (ret == 0 && put_result == BAMBOO_OK)
	return 0;
    else {
	ERROR("put for " << key << " ultimately failed: put_result = " << put_result << ", retry_count = " << no);
	return E_RPC_FAILURE;
    }    
}

/* 
template<class K, class V>
typename bamboo_dht<K, V>::enumeration_type bamboo_dht<K, V>::getEnumeration(pkey_type key) {
    int no = 0;
    CLIENT *clnt = getRPCConnection(&no);
    if (clnt == NULL) {
	cout << "Cannot establish get connection for " << key << endl;
	return boost::shared_ptr<buffered_enumeration<V> >();
    }

    bamboo_key sha_key;

    // set the key (SHA1 of key)
    std::stringstream kstream(std::ios_base::binary | std::ios_base::out);
    boost::archive::binary_oarchive ks(kstream);
    ks << *key;
    std::string key_representation = kstream.str();
    SHA1((unsigned char *)key_representation.data(), key_representation.size(), (unsigned char *)sha_key);

    // return a new enumeration
    bamboo_enumeration<V> *e = new bamboo_enumeration<V>(sha_key, clnt);    
    return boost::shared_ptr<buffered_enumeration<V> >(e);
}
*/

int bamboo_dht::remove(pkey_type key, pvalue_type value, int ttl, const std::string &secret) {
    int no = 0, gid = -1;

    // set the general arguments
    bamboo_rm_arguments rm_args;
    rm_args.application = APP_STRING;
    rm_args.client_library = CLIB_STRING;
    rm_args.ttl_sec = ttl; 

    // if the value representation does not fit in the DHT limits fail
    if (value.size() > VALUE_SIZE_MAX)
	return E_MAXVALUE_EXCEEDED;

    // set the SHA1 of key
    SHA1((unsigned char *)key.get(), key.size(), (unsigned char *)rm_args.key);

    // set the SHA1 of value
    rm_args.value_hash.algorithm = HASH_DESC;
    rm_args.value_hash.hash.hash_len = HASH_LEN;
    rm_args.value_hash.hash.hash_val = (char *)malloc(HASH_LEN);
    SHA1((unsigned char *)value.get(), value.size(), (unsigned char *)rm_args.value_hash.hash.hash_val);
    
    // set the secret
    rm_args.secret_hash_alg = HASH_DESC;
    if (secret != "") {
	rm_args.secret.secret_len = secret.size();
	rm_args.secret.secret_val = (char *)secret.data();
    } else {
	rm_args.secret.secret_len = strlen(HASH_DESC);
	rm_args.secret.secret_val = HASH_DESC;
    }
   
    bamboo_stat rm_result = BAMBOO_AGAIN;
    int ret = 0;
    CLIENT *clnt = NULL;
    while ((ret != 0 || rm_result == BAMBOO_AGAIN) && no < this->retry_count) {
	clnt = getRPCConnection(no, gid);
	if (clnt == NULL)
	    continue;
	ret = bamboo_dht_proc_rm_3(&rm_args, &rm_result, clnt);
	clnt_destroy(clnt);
    }

    free(rm_args.value_hash.hash.hash_val);
    return (ret == 0 && rm_result == BAMBOO_OK) ? 0 : E_RPC_FAILURE;
}
