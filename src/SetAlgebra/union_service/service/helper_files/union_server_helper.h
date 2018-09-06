#ifndef __ROUTER_SERVER_HELPER_H_INCLUDED__
#define __ROUTER_SERVER_HELPER_H_INCLUDED__

#include "protoc_files/union.grpc.pb.h"
#include "intersection_service/service/helper_files/client_helper.h"
#include "union_service/src/thread_safe_queue.cpp"
#include "union_service/src/thread_safe_flag.cpp"
#include "union_service/src/thread_safe_circ_buffer.cpp"
#include "union_service/src/atomics.cpp"

#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}

typedef long long Docids;
typedef long long Wordids;

/* Struct contains necessary info for each worker thread. 
   Each worker thread launches a grpc connection to a 
   corresponding intersection server. */
struct ThreadArgs {
    std::vector<Wordids> word_ids;
    std::vector<Docids> posting_list;
    IntersectionSrvTimingInfo intersection_srv_timing_info;
    IntersectionSrvUtil intersection_srv_util;
};

struct ResponseMetaData {
    std::vector<ResponseData> response_data;
    uint64_t responses_recvd = 0;
    uint64_t id = 0;
    union_service::UnionResponse* union_reply = new union_service::UnionResponse();
};

struct DispatchedData {
    void* tag = NULL;
    int union_tid = 0;
};

struct ReqToIntersectionSrv {
    std::vector<Wordids> word_ids;
    std::vector<Docids>* posting_list = new std::vector<Docids>();
    int union_tid = 0;
    bool util_present = false;
    intersection::IntersectionRequest request_to_intersection_srv;
    IntersectionSrvTimingInfo* intersection_srv_timing_info;
    IntersectionSrvUtil* intersection_srv_util;
    int intersection_server_id = 0;
    uint64_t request_id = 0;
};

struct mutex_wrapper : std::mutex
{
    mutex_wrapper() = default;
    mutex_wrapper(mutex_wrapper const&) noexcept : std::mutex() {}
    bool operator==(mutex_wrapper const&other) noexcept { return this==&other; }
};


struct ThreadSafeQueueReqWrapper : ThreadSafeQueue<ReqToIntersectionSrv>
{
    ThreadSafeQueueReqWrapper() = default;
    ThreadSafeQueueReqWrapper(ThreadSafeQueueReqWrapper const&) noexcept : ThreadSafeQueue<ReqToIntersectionSrv>() {}
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
    TMConfig(int num_inline, int num_workers, int num_resps, std::vector<std::thread> inline_thread_pool = {}, std::vector<std::thread> intersection_client_thread_pool = {}, std::vector<std::thread> worker_thread_pool = {}, std::vector<std::thread> resp_thread_pool = {})
        : num_inline(num_inline)
        , num_workers(num_workers)
        , num_resps(num_resps)
        , inline_thread_pool(std::move(inline_thread_pool))
        , intersection_client_thread_pool(std::move(intersection_client_thread_pool))
        , worker_thread_pool(std::move(worker_thread_pool))
        , resp_thread_pool(std::move(resp_thread_pool))
        {}
    int num_inline = 1;
    int num_workers = 0;
    int num_resps = 0;
    std::vector<std::thread> inline_thread_pool;
    std::vector<std::thread> intersection_client_thread_pool;
    std::vector<std::thread> worker_thread_pool;
    std::vector<std::thread> resp_thread_pool;
};

// uint64_t refers to the void* to the request's tag - i.e its unique id
typedef std::map<uint64_t, ResponseMetaData> ResponseMap;

/* Bucket server IPs are taken in via a file. This file must be read,
   and the intersection server IPs must be stored in a vector of strings. 
   This is so that different point IDs can be suitably routed to
   different intersection servers (based on the shard).
In: string - intersection server IPs file name
Out: vector of strings - all the intersection server IPs*/
void GetIntersectionServerIPs(const std::string &intersection_server_ips_file, 
        std::vector<std::string>* intersection_server_ips);

void UnpackUnionServiceRequest(const union_service::UnionRequest &union_request,
        std::vector<Wordids>* word_ids,
        intersection::IntersectionRequest* request_to_intersection_srv);

void Merge(const struct ThreadArgs* thread_args,
        const unsigned int number_of_intersection_servers,
        std::vector<Docids>* final_posting_list,
        uint64_t* create_intersection_srv_req_time,
        uint64_t* unpack_intersection_srv_resp_time,
        uint64_t* unpack_intersection_srv_req_time,
        uint64_t* calculate_intersection_time,
        uint64_t* pack_intersection_srv_resp_time);

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const unsigned int number_of_intersection_servers,
        union_service::UnionResponse* union_reply);

void PackUnionServiceResponse(const std::vector<Docids> &final_posting_list,
        const struct ThreadArgs* thread_args,
        const unsigned int number_of_intersection_servers,
        union_service::UnionResponse* union_reply);


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
