#include <iostream>
#include <stdio.h>
#include <string>
#include <libmemcached/memcached.h>

#include "protoc_files/lookup.grpc.pb.h"

#include "lookup_service/src/thread_safe_map.cpp"

#ifndef __CLIENT_HELPER_H_INCLUDED__
#define __CLIENT_HELPER_H_INCLUDED__
#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}

void CreateMemcachedConn(const int memcached_port,
        memcached_st *memc,
        memcached_return* rc);

void UnpackBucketServiceRequest(const lookup::Key &request,
        uint32_t* operation,
        std::string* key,
        std::string* value);

void Get(memcached_st* memc,
        memcached_return* rc,
        const std::string key,
        std::string* value);

void Set(memcached_st* memc,
        memcached_return* rc,
        const std::string key,
        std::string* value);

#if 0
void Get(memcached_st* memc,
        memcached_return* rc,
        ThreadSafeMap &key_value_store,
        std::string key,
        std::string* value);

void Set(memcached_st* memc,
        memcached_return* rc,
        ThreadSafeMap* key_value_store,
        std::string key,
        std::string* value);
#endif

void Update(const std::string key,
        const std::string value);

#endif // __CLIENT_HELPER_H_INCLUDED__

