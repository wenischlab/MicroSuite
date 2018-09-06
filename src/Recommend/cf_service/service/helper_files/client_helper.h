#ifndef __CLIENT_HELPER_H_INCLUDED__
#define __CLIENT_HELPER_H_INCLUDED__

#include "protoc_files/cf.grpc.pb.h"

struct CFSrvTimingInfo {
    uint64_t create_cf_srv_request_time = 0;
    uint64_t unpack_cf_srv_req_time = 0;
    uint64_t cf_srv_time = 0;
    uint64_t pack_cf_srv_resp_time = 0;
    uint64_t unpack_cf_srv_resp_time = 0;
    float cpu_util_cf_srv = 0.0;
};

struct CFSrvUtil {
    bool util_present = false;
    uint64_t user_time = 0;
    uint64_t system_time = 0;
    uint64_t io_time = 0;
    uint64_t idle_time = 0;
};

struct CFClientCommandLineArgs{
    unsigned number_of_nearest_neighbors = 0;
};

struct ResponseData {
    float* rating = new float();
    CFSrvTimingInfo* cf_srv_timing_info = new CFSrvTimingInfo();
    CFSrvUtil* cf_srv_util = new CFSrvUtil();
};

void CreateCFServiceRequest(const uint32_t cf_server_id,
        const bool util_present,
        collaborative_filtering::CFRequest* request_to_cf_srv);

void UnpackCFServiceResponse(const collaborative_filtering::CFResponse &reply,             
        float* rating,
        CFSrvTimingInfo* cf_srv_timing_info,
        CFSrvUtil* cf_srv_util);

/* Following two functions are just helper unpack
   functions to the function above.*/
void UnpackTimingInfo(const collaborative_filtering::CFResponse &reply,
        CFSrvTimingInfo* cf_srv_timing_info);

void UnpackUtilInfo(const collaborative_filtering::CFResponse &reply,
        CFSrvUtil* cf_srv_util);

#if 0

/* Parses command line args and puts them into a CFClientCommandLineArgs
   struct. 
In: user command line input 
Out: pointer to struct CFClientCommandLineArgs.
A.S As of now, the command line accepts number of nearest neighbors to be computed.*/
CFClientCommandLineArgs* ParseCFClientCommandLine(const int &argc, 
        char** argv);
// A.S Spare function - not using it any more.
void ReadPointIDsFromFile(const std::string &point_ids_file_name, 
        std::vector<std::vector<uint32_t> >* point_ids);
/* Packs data that needs to be sent to the bucket server, into a 
   protobuf message. 
In: queries, point IDs, queries_size, number of neighbors that must
be computed, number of dimensions of each point, ID of the bucket server,
shard size.
Out: Protobuf message - request.*/ 
void CreateCFServiceRequest(const MultiplePoints &queries, 
        const std::vector<std::vector<uint32_t>> &point_ids, 
        const unsigned queries_size, 
        const unsigned number_of_nearest_neighbors, 
        const int dimension,
        const uint32_t bucket_server_id,
        const int shard_size,
        const bool util_present,
        bucket::NearestNeighborRequest* request);
/* Convert the bucket server's protobuf response message
   into a bunch of different response values.
In: Reply from the bucket server, number of nearest neighbors.
Out: k-nn answers for all queries, timing info: unpacking bucket server req,
knn distance calc time, packing bucket server respose time (in micro).*/ 
void UnpackCFServiceResponse(const bucket::NearestNeighborResponse &reply, 
        const unsigned number_of_nearest_neighbors,
        DistCalc* knn_answer,
        CFTimingInfo* bucket_timing_info,
        CFUtil* bucket_util);

/* Following two functions are just helper unpack
   functions to the function above.*/
void UnpackTimingInfo(const bucket::NearestNeighborResponse &reply,
        CFTimingInfo* bucket_timing_info);

void UnpackUtilInfo(const bucket::NearestNeighborResponse &reply,
        CFUtil* bucket_util);

void AddCFResponseToMap();

/* Create an output text file containing k-nn for all input queries.
   Program exits if file cannot be opened.
In: file name, final k-nn answer for all queries.*/
void WriteKNNToFile(const std::string &knn_file_name, 
        const DistCalc &knn_answer);
/* Prints contents of an object of type: DistCalc.
In: object of type DistCalc.*/
void PrintKNNForAllQueries(const DistCalc &knn_answer);
/* Prints point IDs returned by the index for all queries.
In: point IDs for all queries.*/ 
void PrintPointIDs(const std::vector<std::vector<uint32_t> > &point_ids);
/* Reads a text file, and converts it into a vector of points.
   Program exits if file canot be opened/ does not exist.
In: name of the text file.
Out: vector of points.*/ 
void CreatePointsFromFile(const std::string &file_name, 
        MultiplePoints* multiple_points);
#endif

#endif // __CLIENT_HELPER_H_INCLUDED__
