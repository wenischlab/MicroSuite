#ifndef __CLIENT_HELPER_H_INCLUDED__
#define __CLIENT_HELPER_H_INCLUDED__

#include "bucket_service/src/dist_calc.h"
#include "protoc_files/bucket.grpc.pb.h"

struct BucketTimingInfo {
    uint64_t create_bucket_request_time = 0;
    uint64_t unpack_bucket_req_time = 0;
    uint64_t calculate_knn_time = 0;
    uint64_t pack_bucket_resp_time = 0;
    uint64_t unpack_bucket_resp_time = 0;
    float cpu_util_bucket = 0.0;
};

struct BucketUtil {
    bool util_present = false;
    uint64_t user_time = 0;
    uint64_t system_time = 0;
    uint64_t io_time = 0;
    uint64_t idle_time = 0;
};

struct BucketClientCommandLineArgs{
    unsigned number_of_nearest_neighbors = 0;
};

struct ResponseData {
    DistCalc* knn_answer = new DistCalc();
    BucketTimingInfo* bucket_timing_info = new BucketTimingInfo();
    BucketUtil* bucket_util = new BucketUtil();
};

/* Parses command line args and puts them into a BucketClientCommandLineArgs
   struct. 
In: user command line input 
Out: pointer to struct BucketClientCommandLineArgs.
A.S As of now, the command line accepts number of nearest neighbors to be computed.*/
BucketClientCommandLineArgs* ParseBucketClientCommandLine(const int &argc, 
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
void CreateBucketServiceRequest(const MultiplePoints &queries, 
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
void UnpackBucketServiceResponse(const bucket::NearestNeighborResponse &reply, 
        const unsigned number_of_nearest_neighbors,
        DistCalc* knn_answer,
        BucketTimingInfo* bucket_timing_info,
        BucketUtil* bucket_util);

/* Following two functions are just helper unpack
   functions to the function above.*/
void UnpackTimingInfo(const bucket::NearestNeighborResponse &reply,
        BucketTimingInfo* bucket_timing_info);

void UnpackUtilInfo(const bucket::NearestNeighborResponse &reply,
        BucketUtil* bucket_util);

void AddBucketResponseToMap();

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

#endif // __CLIENT_HELPER_H_INCLUDED__
