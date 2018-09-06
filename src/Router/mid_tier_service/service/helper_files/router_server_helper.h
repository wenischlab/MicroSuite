#ifndef __ROUTER_SERVER_HELPER_H_INCLUDED__
#define __ROUTER_SERVER_HELPER_H_INCLUDED__

#include "protoc_files/router.grpc.pb.h"
#include "lookup_service/service/helper_files/client_helper.h"
#include "mid_tier_service/src/thread_safe_circ_buffer.cpp"
#include "mid_tier_service/src/thread_safe_queue.cpp"
#include "mid_tier_service/src/thread_safe_flag.cpp"
#include "mid_tier_service/src/atomics.cpp"
#include "mid_tier_service/src/spookyhash.h"

#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}

/* Struct contains necessary info for each worker thread. 
   Each worker thread launches a grpc connection to a 
   corresponding bucket server. */
struct ThreadArgs {
    std::string operation;
    std::string key;
    std::string value;
    LookupSrvTimingInfo lookup_srv_timing_info;
    LookupSrvUtil lookup_srv_util;
};

struct ResponseMetaData {
    std::vector<ResponseData> response_data;
    int responses_recvd = 0;
    uint64_t id = 0;
    router::LookupResponse* router_reply = new router::LookupResponse();
};

struct DispatchedData {
    void* tag = NULL;
    int router_tid = 0;
};

struct ReqToLookupSrv {
    std::string key = "";
    std::string* value;
    int operation = 0;
    int router_tid = 0;
    bool util_present = false;
    lookup::Key request_to_lookup_srv;
    LookupSrvTimingInfo* lookup_srv_timing_info;
    LookupSrvUtil* lookup_srv_util;
    int lookup_server_id = 0;
    uint64_t request_id = 0;
};

struct mutex_wrapper : std::mutex
{
    mutex_wrapper() = default;
    mutex_wrapper(mutex_wrapper const&) noexcept : std::mutex() {}
    bool operator==(mutex_wrapper const&other) noexcept { return this==&other; }
};


struct ThreadSafeQueueReqWrapper : ThreadSafeQueue<ReqToLookupSrv>
{
    ThreadSafeQueueReqWrapper() = default;
    ThreadSafeQueueReqWrapper(ThreadSafeQueueReqWrapper const&) noexcept : ThreadSafeQueue<ReqToLookupSrv>() {}
    bool operator==(ThreadSafeQueueReqWrapper const&other) noexcept {return this==&other; }
};

struct ThreadSafeQueueRespWrapper : ThreadSafeQueue<bool>
{
    ThreadSafeQueueRespWrapper() = default;
    ThreadSafeQueueRespWrapper(ThreadSafeQueueRespWrapper const&) noexcept : ThreadSafeQueue<bool>() {}
    bool operator==(ThreadSafeQueueRespWrapper const&other) noexcept {return this==&other; }
};

struct ThreadSafeFlagWrapper : ThreadSafeFlag<bool>
{
    ThreadSafeFlagWrapper() = default;
    ThreadSafeFlagWrapper(ThreadSafeFlagWrapper const&) noexcept : ThreadSafeFlag<bool>() {}
    bool operator==(ThreadSafeFlagWrapper const&other) noexcept {return this==&other; }
};

struct TMConfig {
    TMConfig(int num_inline, int num_workers, int num_resps, std::vector<std::thread> inline_thread_pool = {}, std::vector<std::thread> bucket_client_thread_pool = {}, std::vector<std::thread> worker_thread_pool = {}, std::vector<std::thread> resp_thread_pool = {})
        : num_inline(num_inline)
        , num_workers(num_workers)
        , num_resps(num_resps)
        , inline_thread_pool(std::move(inline_thread_pool))
        , bucket_client_thread_pool(std::move(bucket_client_thread_pool))
        , worker_thread_pool(std::move(worker_thread_pool))
        , resp_thread_pool(std::move(resp_thread_pool))
        {}
    int num_inline = 1;
    int num_workers = 0;
    int num_resps = 0;
    std::vector<std::thread> inline_thread_pool;
    std::vector<std::thread> bucket_client_thread_pool;
    std::vector<std::thread> worker_thread_pool;
    std::vector<std::thread> resp_thread_pool;
};

// uint64_t refers to the void* to the request's tag - i.e its unique id
typedef std::map<uint64_t, ResponseMetaData> ResponseMap;

/* Bucket server IPs are taken in via a file. This file must be read,
   and the bucket server IPs must be stored in a vector of strings. 
   This is so that different point IDs can be suitably routed to
   different bucket servers (based on the shard).
In: string - bucket server IPs file name
Out: vector of strings - all the bucket server IPs*/
void GetLookupServerIPs(const std::string &lookup_server_ips_file, 
        std::vector<std::string>* lookup_server_ips);

void UnpackRouterServiceRequest(const router::RouterRequest &router_request,
        std::string* key,
        std::string* value,
        uint32_t* operation,
        lookup::Key* request_to_lookup_srv);


void Merge(struct ThreadArgs* thread_args,
        unsigned int replication_cnt,
        std::string* lookup_val,
        uint64_t* create_lookup_srv_req_time,
        uint64_t* unpack_lookup_srv_resp_time,
        uint64_t* unpack_lookup_srv_req_time,
        uint64_t* lookup_srv_time,
        uint64_t* pack_lookup_srv_resp_time,
        router::LookupResponse* router_reply);

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const int replication_cnt,
        router::LookupResponse* router_reply);

// Following list of functions apply only to the auto tuner.
void InitializeTMs(const int num_tms, 
        std::map<TMNames, TMConfig>* all_tms);
void InitializeAsyncTMs(const int num_tms,
        std::map<AsyncTMNames, TMConfig>* all_tms);

void InitializeFMSyncTMs(const int num_tms,
        std::map<FMSyncTMNames, TMConfig>* all_tms);
void InitializeFMAsyncTMs(const int num_tms,
        std::map<FMAsyncTMNames, TMConfig>* all_tms);

#endif //__LOADGEN_INDEX_SERVER_HELPER_H_INCLUDED__
