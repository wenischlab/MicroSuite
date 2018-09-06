#ifndef __LOADGEN_INDEX_CLIENT_HELPER_H_INCLUDED__
#define __LOADGEN_INDEX_CLIENT_HELPER_H_INCLUDED__

#include "protoc_files/mid_tier.grpc.pb.h"
#include "utils.h"

#define MD5_DIGEST_LENGTH 16

/* This structure holds the timing info from all pieces of the system:
   load generator, index, and the bucket. Individual timing elements 
   are filled up by grpc info. All timing data is in microseconds.*/
struct TimingInfo
{
    uint64_t create_queries_time = 0;
    uint64_t create_index_req_time = 0;
    uint64_t update_index_util_time = 0;
    uint64_t unpack_index_resp_time = 0;
    uint64_t unpack_loadgen_req_time = 0;
    uint64_t get_point_ids_time = 0;
    uint64_t get_bucket_responses_time = 0;
    uint64_t create_bucket_req_time = 0;
    uint64_t unpack_bucket_req_time = 0;
    uint64_t calculate_knn_time = 0;
    uint64_t pack_bucket_resp_time = 0;
    uint64_t unpack_bucket_resp_time = 0;
    uint64_t merge_time = 0;
    uint64_t pack_index_resp_time = 0;
    uint64_t total_resp_time = 0;
    uint64_t index_time = 0;
};

/* This structure holds the data entered by the load generator user.*/
struct LoadGenCommandLineArgs
{
    std::string queries_file_name = "";
    std::string result_file_name = "";
    unsigned int number_of_nearest_neighbors = 0;
    uint64_t time_duration = 0;
    float qps = 0;
    std::string ip = "localhost";
    std::string qps_file_name = "";
    std::string timing_file_name = "";
    std::string util_file_name = "";
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
    Util* bucket_util;
    Util index_util;
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
    PercentUtil* bucket_util_percent;
    PercentUtil index_util_percent;
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

/* For the loadgen that interfaces with the frontend, we need to
   compute the checksum of a given jpeg file, so that we can cache it
   This is so that we don't have to extract the feature vector if an
   image was already seen (in the cache).
In: jpeg file name - the query
Out: character array - md5 checksum.*/

void ComputeMD5(const std::string &queries_file_name, std::string* checksum);

/* Check redis to see if the file has already been seen
   and therefore has been cached.
In: Checksum - key to redis.
Out: True (file is cached), or False - file hsn't been seen.*/
bool FilePresent(const std::string &checksum);

/* Given that redis has the jpeg file, get the feature vector 
   in the form of multiple points. Redis returns the entire
   feature vector in the form of a string. We must convert this
   string into multiple points.*/
void GetFeatureVector(const std::string &checksum, Point* query);

void CallFeatureExtractor(const std::string &queries_file_name, 
        const std::string &checksum,
        Point* query);

/* Create a grpc request to the index server.
   Packs all relevant information into a protobuf 
   data structure that can then be passed to the index server.
In: queries, size of queries, number of nearest
neighbors to be computed, dimension of each data point.
Out: load generator's request to the index server.*/ 
void CreateIndexServiceRequest(const MultiplePoints &queries, 
        const uint64_t query_id,
        const unsigned &queries_size,
        const unsigned &number_of_nearest_neighbors,
        const int &dimension,
        const bool util_request,
        loadgen_index::LoadGenRequest* load_gen_request);

/* Unpack the protobuf response message that was sent by the index server
   via grpc and add the values unpacked to corresponding variables.
In: response from the index server, number of queries, and the
number of nearest neighbors to be computed.
Out: final k-NN answer for all queries, timing information from
individual index and bucket pieces.*/
void UnpackIndexServiceResponse(const loadgen_index::ResponseIndexKnnQueries &index_reply, 
        DistCalc* knn_answer, 
        TimingInfo* timing_info,
        UtilInfo* util,
        PercentUtilInfo* percent_util);

/* Following two functions are helpers to the above function:
   They unpack stats.*/
void UnpackTimingInfo(const loadgen_index::ResponseIndexKnnQueries &index_reply,
        TimingInfo* timing_info);

void UnpackUtilInfo(const loadgen_index::ResponseIndexKnnQueries &index_reply,
        UtilInfo* util,
        PercentUtilInfo* percent_util);

/* Helped funstion: Print the K-NN result for all queries. 
   The result is like a 3D matrix. A set of K- nearest 
   neighbor points (each point has multiple dimensions),
   for every query.
In: K-NN for all queries.
Out: outputs K-NN for all queries to the terminal.*/
void PrintKNNForAllQueries(const DistCalc &knn_answer);

/* Helper function: Print the point IDs for every query.
   These are the point IDs sent by the index to the bucket and 
   the bucket must look up corresponding dataset entries in its shard,
   and perform distance computations.
In: point IDs for every query.
Out: prints point IDs for every query, to terminal.*/
void PrintPointIDs(const std::vector<std::vector<uint32_t> > &point_ids_vec);

/* Given a file path to a text file, open the file, and create 
   a set of points.
In: path to text file.
Out: a set of points created.*/
void CreatePointsFromFile(const std::string &file_name, 
        MultiplePoints* multiple_points);

/* Large query files are better stored as binary files.
   Adding support to create a large query set from a binary file.*/
void CreatePointsFromBinFile(const std::string &file_name,
        MultiplePoints* multiple_points);

/* Write the K-NN received by the load generator to a text file.
In: path to the text file that must be written to.
In: final answer - K-NN for all queries.*/
void WriteKNNToFile(const std::string &knn_file_name, const DistCalc &knn_answer);

/* Print the timing information that is received by grpc from all individual 
   distributed system components - load generator, index and bucket servers.
In: timing information structure.
Out: print timing information to terminal.*/
void PrintStatistics(const TimingInfo &timing_info);

void WriteStatisticsToFile(std::ofstream &timing_file, const TimingInfo &timing_info);

void WriteTimingInfoToFile(std::ofstream &timing_file, const TimingInfo &timing_info);

void UpdateGlobalTimingStats(const TimingInfo &timing_info,
        GlobalStats* global_stats);

void UpdateGlobalUtilStats(PercentUtilInfo* percent_util_info,
        const unsigned int number_of_bucket_servers,
        GlobalStats* global_stats);

float ComputeQueryCost(const GlobalStats &global_stats,
                       const unsigned int util_requests,
                       const unsigned int number_of_bucket_servers,
                       float achieved_qps);

void PrintGlobalStats(const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd);

void PrintLatency(const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd);

void PrintUtil(const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests);


void WriteToUtilFile(const std::string util_file_name,
        const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests);

void PrintTimingHistogram(std::vector<uint64_t> &time_vec);

void ResetMetaStats(GlobalStats* meta_stats,
        int number_of_bucket_servers);

#endif // __LOADGEN_INDEX_CLIENT_HELPER_H_INCLUDED__
