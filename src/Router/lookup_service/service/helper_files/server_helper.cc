#include "server_helper.h"

std::mutex test;

void CreateMemcachedConn(const int memcached_port,
        memcached_st* memc,
        memcached_return* rc)
{

    memcached_server_st *servers = NULL;

    memcached_server_st *memcached_servers_parse (char *server_strings);
    servers = memcached_server_list_append(servers, "localhost", memcached_port, rc);
    *rc = memcached_server_push(memc, servers);

    if (*rc == MEMCACHED_SUCCESS) {
        fprintf(stderr,"Added server successfully\n");
    } else {
        CHECK(false, "Could not add server\n");
    }
}

void UnpackBucketServiceRequest(const lookup::Key &request,
        uint32_t* operation,
        std::string* key,
        std::string* value)
{
    *operation = request.operation();
    *key = request.key();
    *value = request.value();
}

void Get(memcached_st* memc,
        memcached_return* rc,
        const std::string key,
        std::string* value)
{
    size_t value_length;
    uint32_t flags;
    char* retrieved_value;
    try {
        test.lock();
        retrieved_value = memcached_get(memc, key.c_str(), strlen(key.c_str()), &value_length, &flags, rc);
        test.unlock();
    } catch(...) {
        CHECK(false, "Exception\n");
    }
    if (*rc == MEMCACHED_SUCCESS) {
        if (retrieved_value == NULL) {
            *(value) = "nack";
            return;
        }
        *(value) = std::string(retrieved_value);
    }
    else {
        *(value) = "nack";
    }
    free(retrieved_value);
}

#if 0
void Get(memcached_st* memc,
        memcached_return* rc,
        ThreadSafeMap &key_value_store,
        std::string key,
        std::string* value)
{
    *value = key_value_store.Get(key);
}

void Set(memcached_st* memc,
        memcached_return* rc,
        ThreadSafeMap* key_value_store,
        std::string key,
        std::string* value)
{
    key_value_store->Set(key, *value);
    *value = "ack";
}
#endif
void Set(memcached_st* memc,
        memcached_return* rc,
        const std::string key,
        std::string* value)
{
    std::string val = *value;

    try {
        test.lock();
        *rc = memcached_set(memc, key.c_str(), strlen(key.c_str()), val.c_str(), strlen(val.c_str()), (time_t)0, (uint32_t)0);
        test.unlock();
    } catch(...) {
        CHECK(false, "Exception\n");
    }

    if (*rc == MEMCACHED_SUCCESS)
    {
        *value = "ack";
    }
    else {
        *value = "nack";
    }
}

void Update(const std::string key,
        const std::string Value)
{

}

