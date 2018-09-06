#ifndef __CLIENT_HELPER_H_INCLUDED__
#define __CLIENT_HELPER_H_INCLUDED__

#include "protoc_files/intersection.grpc.pb.h"

typedef long long Docids;

struct IntersectionSrvTimingInfo {
    uint64_t create_intersection_srv_request_time = 0;
    uint64_t unpack_intersection_srv_req_time = 0;
    uint64_t calculate_intersection_time = 0;
    uint64_t pack_intersection_srv_resp_time = 0;
    uint64_t unpack_intersection_srv_resp_time = 0;
    float cpu_util_intersection_srv = 0.0;
};

struct IntersectionSrvUtil {
    bool util_present = false;
    uint64_t user_time = 0;
    uint64_t system_time = 0;
    uint64_t io_time = 0;
    uint64_t idle_time = 0;
};

struct IntersectionClientCommandLineArgs{
    unsigned number_of_nearest_neighbors = 0;
};

struct ResponseData {
    std::vector<Docids>* posting_list = new std::vector<Docids>();
    IntersectionSrvTimingInfo* intersection_srv_timing_info = new IntersectionSrvTimingInfo();
    IntersectionSrvUtil* intersection_srv_util = new IntersectionSrvUtil();
};

void CreateIntersectionServiceRequest(const uint32_t intersection_server_id,
        const bool util_present,
        intersection::IntersectionRequest* request_to_intersection_srv);

void UnpackIntersectionServiceResponse(const intersection::IntersectionResponse &reply,             
        std::vector<Docids>* posting_list,
        IntersectionSrvTimingInfo* intersection_srv_timing_info,
        IntersectionSrvUtil* intersection_srv_util);

/* Following two functions are just helper unpack
   functions to the function above.*/
void UnpackTimingInfo(const intersection::IntersectionResponse &reply,
        IntersectionSrvTimingInfo* intersection_srv_timing_info);

void UnpackUtilInfo(const intersection::IntersectionResponse &reply,
        IntersectionSrvUtil* intersection_srv_util);

#if 0

/* Parses command line args and puts them into a IntersectionClientCommandLineArgs
   struct. 
In: user command line input 
Out: pointer to struct IntersectionClientCommandLineArgs.
A.S As of now, the command line accepts number of nearest neighbors to be computed.*/
IntersectionClientCommandLineArgs* ParseIntersectionClientCommandLine(const int &argc, 
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
void CreateIntersectionServiceRequest(const MultiplePoints &queries, 
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
void UnpackIntersectionServiceResponse(const bucket::NearestNeighborResponse &reply, 
        const unsigned number_of_nearest_neighbors,
        DistCalc* knn_answer,
        IntersectionTimingInfo* bucket_timing_info,
        IntersectionUtil* bucket_util);

/* Following two functions are just helper unpack
   functions to the function above.*/
void UnpackTimingInfo(const bucket::NearestNeighborResponse &reply,
        IntersectionTimingInfo* bucket_timing_info);

void UnpackUtilInfo(const bucket::NearestNeighborResponse &reply,
        IntersectionUtil* bucket_util);

void AddIntersectionResponseToMap();

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
