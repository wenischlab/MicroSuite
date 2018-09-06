#ifndef __LOADGEN_UNION_CLIENT_HELPER_H_INCLUDED__
#define __LOADGEN_UNION_CLIENT_HELPER_H_INCLUDED__

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <streambuf>
#include <sstream>
#include "protoc_files/recommender.grpc.pb.h"

#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}

struct TimingInfo
{
    uint64_t create_queries_time = 0;
    uint64_t create_recommender_req_time = 0;
    uint64_t update_recommender_util_time = 0;
    uint64_t unpack_recommender_resp_time = 0;
    uint64_t unpack_recommender_req_time = 0;
    uint64_t get_cf_srv_responses_time = 0;
    uint64_t create_cf_srv_req_time = 0;
    uint64_t unpack_cf_srv_req_time = 0;
    uint64_t calculate_cf_time = 0;
    uint64_t pack_cf_srv_resp_time = 0;
    uint64_t unpack_cf_srv_resp_time = 0;
    uint64_t pack_recommender_resp_time = 0;
    uint64_t total_resp_time = 0;
    uint64_t recommender_time = 0;
};

/* This structure holds the data entered by the load generator user.*/
struct LoadGenCommandLineArgs
{
    std::string queries_file_name = "";
    std::string result_file_name = "";
    uint64_t time_duration = 0;
    float qps = 0;
    std::string ip = "localhost";
};

struct Util
{
    uint64_t user_time = 0;
    uint64_t system_time = 0;
    uint64_t io_time = 0;
    uint64_t idle_time = 0;
};

struct UtilInfo
{
    bool util_info_present = false;
    Util* cf_srv_util;
    Util recommender_util;
};

struct PercentUtil
{
    float user_util = 0.0;
    float system_util = 0.0;
    float io_util = 0.0;
    float idle_util = 0.0;
};

struct PercentUtilInfo
{
    PercentUtil* cf_srv_util_percent;
    PercentUtil recommender_util_percent;
};

struct GlobalStats
{
    std::vector<TimingInfo> timing_info;
    PercentUtilInfo percent_util_info;
};

/* Parse the user input and extract relevant data from it.
   Relevant data is in the form of the queries file path,
   path for the file that will hold the K-NN result, and the 
   number of nearest neighbors that must be computed.
In: number of command line arguments and the arguments themselves.
Out: A pointer to the struct "LoadGenCommandLineArgs" that holds
relevant info.*/
LoadGenCommandLineArgs* ParseLoadGenCommandLine(const int &argc,
        char** argv);

void CreateQueriesFromFile(std::string queries_file_name,
        std::vector<std::pair<int, int> >* queries);

void CreateRecommenderServiceRequest(const std::pair<int, int> query,
        const bool util_request,                
        recommender::RecommenderRequest* recommender_request);

void UnpackRecommenderServiceResponse(const recommender::RecommenderResponse &recommender_reply,
        float* rating,
        TimingInfo* timing_info,                                                                                                                                                                                                                            
        UtilInfo* previous_util,                                                                                                                                                                                                                                                          
        PercentUtilInfo* percent_util_info);  

/* Following two functions are helpers to the above function:
   They unpack stats.*/
void UnpackTimingInfo(const recommender::RecommenderResponse &recommender_reply,
        TimingInfo* timing_info);

void UnpackUtilInfo(const recommender::RecommenderResponse &recommender_reply,
        UtilInfo* util,
        PercentUtilInfo* percent_util);

void PrintRatingForAllQueries(const float rating);

void UpdateGlobalTimingStats(const TimingInfo &timing_info,
        GlobalStats* global_stats);

void UpdateGlobalUtilStats(PercentUtilInfo* percent_util_info,
        const unsigned int number_of_cf_servers,
        GlobalStats* global_stats);

void PrintTime(std::vector<uint64_t> time_vec);

float ComputeQueryCost(const GlobalStats &global_stats,
        const unsigned int util_requests,
        const unsigned int number_of_cf_servers,
        float achieved_qps);

void PrintGlobalStats(const GlobalStats &global_stats,
        const unsigned int number_of_cf_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd);

void PrintLatency(const GlobalStats &global_stats,
        const unsigned int number_of_cf_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd);

void PrintUtil(const GlobalStats &global_stats,
        const unsigned int number_of_cf_servers,
        const unsigned int util_requests);

void PrintTimingHistogram(std::vector<uint64_t> &time_vec);

void ResetMetaStats(GlobalStats* meta_stats,
        int number_of_cf_servers);
#endif // __LOADGEN_INDEX_CLIENT_HELPER_H_INCLUDED__
