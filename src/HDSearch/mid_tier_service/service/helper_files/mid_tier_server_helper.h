/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#ifndef __LOADGEN_INDEX_SERVER_HELPER_H_INCLUDED__
#define __LOADGEN_INDEX_SERVER_HELPER_H_INCLUDED__

#include <flann/flann.hpp>
#include "protoc_files/mid_tier.grpc.pb.h"
#include "bucket_service/service/helper_files/client_helper.h"
#include "bucket_service/src/dist_calc.h"
#include "mid_tier_service/src/thread_safe_circ_buffer.cpp"
#include "mid_tier_service/src/thread_safe_queue.cpp"
#include "mid_tier_service/src/thread_safe_flag.cpp"
#include "mid_tier_service/src/atomics.cpp"

#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}
struct ResponseMetaData {
    std::vector<ResponseData> response_data;
    int responses_recvd = 0;
    uint64_t id = 0;
    loadgen_index::ResponseIndexKnnQueries* index_reply = new loadgen_index::ResponseIndexKnnQueries();
};
#if 0
class ThreadSafeMap {
    public:
        ThreadSafeMap() = default;

        // Multiple threads/readers can read the counter's value at the same time.
        void Get(uint64_t key, ResponseMetaData* value) {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            try {
                map_.at(key);
            } catch( ... ) {
                value = NULL;
            }
            *value = map_[key];
        }

        // Only one thread/writer can increment/write the counter's value.
        void Set(uint64_t key, ResponseMetaData &value) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            map_[key] = value;
        }

    private:
        mutable std::shared_mutex mutex_;
        std::map<uint64_t, ResponseMetaData> map_;
};
#endif

/* Struct contains necessary info for each worker thread. 
   Each worker thread launches a grpc connection to a 
   corresponding bucket server. The worker thread sends 
   queries, corresponding point IDs for each query, number of nearest
   neighbors to be computed, the bucket server ID and the shard size
   (necessary for the bucket server to know which piece of the
   dataset it must bring into memory).*/
struct ThreadArgs {
    std::vector<std::vector<uint32_t> > point_ids;
    DistCalc knn_answer;
    BucketTimingInfo bucket_timing_info;
    BucketUtil bucket_util;
};

/* Contains data from the user. Includes LSH parameters
   that need to be modified depending on accuracy/performance
   requirements. 
   A.S If the command line arguments keep increasing, I'm considering
   reading them from a file.*/
struct IndexServerCommandLineArgs {
    unsigned int num_hash_tables = 1;
    unsigned int hash_table_key_length = 5;
    unsigned int num_multi_probe_levels = 1;
    unsigned int number_of_bucket_servers = 1;
    std::string dataset_file_name = "";
    int mode = 1;
    std::string bucket_server_ips_file = "";
    std::string ip = "localhost";
    int network_poller_parallelism = 0;
    int dispatch_parallelism = 0;
    int number_of_async_response_threads = 0;
    int get_profile_stats = 0;
};

struct Key {
    char key[16];
};

struct DispatchedData {
    void* tag = NULL;
    int index_tid = 0;
};

struct ReqToBucketSrv {
    int index_tid = 0;
    MultiplePoints queries_multiple_points;
    uint64_t query_id = 0;
    unsigned int shard_size = 0;
    std::vector<std::vector<uint32_t> > point_ids;
    unsigned int queries_size = 0;
    unsigned int number_of_nearest_neighbors = 0;
    bool util_present = false;
    bucket::NearestNeighborRequest request_to_bucket;
    DistCalc* knn_answer;
    BucketTimingInfo* bucket_timing_info;
    BucketUtil* bucket_util;
    int bucket_server_id = 0;
    uint64_t request_id = 0;
};

struct mutex_wrapper : std::mutex
{
    mutex_wrapper() = default;
    mutex_wrapper(mutex_wrapper const&) noexcept : std::mutex() {}
    bool operator==(mutex_wrapper const&other) noexcept { return this==&other; }
};


struct ThreadSafeQueueReqWrapper : ThreadSafeQueue<ReqToBucketSrv>
{
    ThreadSafeQueueReqWrapper() = default;
    ThreadSafeQueueReqWrapper(ThreadSafeQueueReqWrapper const&) noexcept : ThreadSafeQueue<ReqToBucketSrv>() {}
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

typedef std::map<uint64_t, ResponseMetaData> ResponseMap;

/* Function converts the command line arguments into corresponding
   LSH parameters and gets the path to the dataset file that must be read
   to construct the LSH index. Throws exception when arguments are
   of an incorrect number, or the "type" of an argument is wrong.
In: number of command line arguments, and a character pointer to the 
arguments themselves.
Out: a struct that holds LSH parameter values, and path to dataset file.*/
IndexServerCommandLineArgs* ParseIndexServerCommandLine(const int argc, 
        char** argv);

/* Bucket server IPs are taken in via a file. This file must be read,
   and the bucket server IPs must be stored in a vector of strings. 
   This is so that different point IDs can be suitably routed to
   different bucket servers (based on the shard).
In: string - bucket server IPs file name
Out: vector of strings - all the bucket server IPs*/
void GetBucketServerIPs(const std::string &bucket_server_ips_file, 
        std::vector<std::string>* bucket_server_ips);

/* Given a set of LSH parameters, and the dataset the index must be build on,
   this function builds the corresponding LSH index (bunch of LSH tables).
In: dataset, number of hash tables that must be constructed, number of 
multi-probe levels (i.e by how many bits must we move, and look at the
corresponding hash table bucket?), hash table key length 
(i.e definition of closeness - selective, or everyone is welcome!).
Out: The corresponding LSH index tables.*/ 
void BuildLshIndex(const flann::Matrix<unsigned char> &dataset,
        const unsigned int num_hash_tables,
        const unsigned int hash_table_key_length,
        const unsigned int num_multi_probe_levels, 
        flann::Index<flann::L2<unsigned char> >* lsh_index);

/* Given a file that contains a saved index, this function
   reads the file and loads the LSH index into memory.
   When the optimal set of LSH parameters are known for a given 
   dataset (optimal in terms of accuracy and performance), it is better
   to save the corresponding LSH index to file and load it the first ever time
   the index server starts running, rather than construct the LSH
   index over and over again (redundant!).
 **A.S. Cost of loading from file is more expensive than constructing the 
 index for some datasets - be aware of this.
In: path to file that contains saved LSH index, and corresponding dataset
Out: LSH index tables.*/
void LoadLshIndexFromFile(const std::string &index_file_name,
        const flann::Matrix<unsigned char> &dataset,
        flann::Index<flann::L2<unsigned char> >* lsh_index);

/* Convert load generator's request (grpc protobuf message) into
   a collection of query points (can either be a batch or single query).
   Function converts float value of each point dimension into
   unsigned char, because flann's LSH supports only unsigned chars.
In: query request(s) from the load generator, number of queries, and
#dimensions of each query point.
Out: collection of query points in 2 formats - Matrix and MultiplePoints.*/
void UnpackLoadgenServiceRequest(const loadgen_index::LoadGenRequest &load_gen_request, 
        const MultiplePoints &dataset,
        const unsigned int queries_size,
        const unsigned int query_dimensions,
        flann::Matrix<unsigned char>* queries, 
        MultiplePoints* queries_multiple_points,
        uint64_t* query_id,
        bucket::NearestNeighborRequest* request_to_bucket);

/* Read a text file and create a collection of points - used to load the 
   dataset into memory. Throws exception if file does not exist or if the
   data is not of type float.
In: path to dataset file
Out: dataset Matrix, number of points in the dataset, and the 
dimension of each point.*/
flann::Matrix<unsigned char>* CreateDatasetFromTextFile(const std::string &file_name, 
        long* dataset_size, 
        unsigned int* dataset_dimensions);

/* Read a binary file and create a collection of points - used to load the 
   dataset into memory. Throws exception if file does not or if the dataset
   size/dimension is a negative value.
In: path to dataset file
Out: dataset Matrix, number of points in the dataset, and the 
dimension of each point.*/
void CreateDatasetFromBinaryFile(const std::string &file_name,
        long* dataset_size,
        unsigned int* dataset_dimensions,
        flann::Matrix<unsigned char>*);

/* Google datset is vailable in the form of a string tag followed by 2048
   dimensions for each point. This function stores the corresponding tags
   separately and creates the dataset separately in the usual manner.
In: path to dataset file
Out: dataset Matrix, number of points in the dataset, the 
dimension of each point, and a vector of string tags.
Note: The index of the vector refers to the index of the dataset point.*/
flann::Matrix<unsigned char>* CreateDatasetWithTagsFromBinaryFile(
        const std::string &file_name,
        long* dataset_size,
        unsigned int* dataset_dimensions,
        std::vector<Key>* keys);


void CreateMultiplePointsFromBinaryFile(const std::string &file_name, MultiplePoints* dataset);

/* Check to see if the dimension of two points is equal.
   Thows exception (exits) otherwise.
 **A.S Function may not be necessary. Just use the CHECK()
 directly.
In: dimension of both points under comparison.
Out: Program calls it quits if dimensions are unequal.*/
void ValidateDimensions(const unsigned int dataset_dimension, 
        const unsigned int point_dimension);

/* It is possible for LSH to not return a candidate. Happens when the 
   hash entry size is chosen to be very small - it's the same as saying
   that only VERY VERY close points get hashed to a particular bucket
   and LSH decides that not of the points are close enough and therefore don't 
   qualify. This function checks to see if such empty entries exist
   for any query.
In: collection of point IDs for each query (considering query batch).
Out: boolean to indicate if empty entries exist/not.*/
bool CheckEmptyPointIDs(const std::vector<std::vector<uint32_t> > &point_ids_vec);

/* If the above mentioned empty entries exist for any query, just populate the
   entry with a default value as "0" i.e the first point in the dataset.
 **A.S Not using this any more. The current system just throws exception
 and quits, in such situations. If you don't want the program to exit,
 call this function.
In: collection of point IDs for each query (considering query batch).
Out: Same vector populated with 0's in all previously empty spots.*/
void PopulatePointIDs(std::vector<std::vector<uint32_t> >* point_ids_vec);

/* Computes the amount of point IDs (out of all the points in the dataset),
   sent to the bucket server for distance computations. This number
   affects the accuracy of the result - higher is better. However, it degrades 
   performance by asking for more distance computations at the bucket nodes.
   Hence, there's a need for a reasonable trade-off to be found.
In: Point IDs for all queries, the number of queries in the batch
(this is 1 if you're sending one query at a time), and the total number
of points in the dataset.
Out: the percent of total Point IDs sent to the bucket.*/
float PercentDataSent(const std::vector<std::vector<uint32_t> > &point_ids,
        const unsigned int queries_size,
        const unsigned int dataset_size);

/* Packs the response from the index server to the load generator into
   a grpc protobuf message. The response contains the "K" nearest
   neighbors for all queries sent by the load generator.
In: the k-NN answer for all queries.
Out: grpc protobuf message containing answers.*/
void PackIndexServiceResponse(const DistCalc &knn_answer, 
        ThreadArgs* thread_args,
        const unsigned int number_of_bucket_servers,
        loadgen_index::ResponseIndexKnnQueries* index_reply);

/* Same as above function, except uses bucket util
   data available from the map of request id - bucket data
   this is used by program for index-bucket async communication.*/
void PackIndexServiceResponseFromMap(const DistCalc &knn_answer,
        const std::vector<ResponseData> &response_data,
        const unsigned int number_of_bucket_servers,
        loadgen_index::ResponseIndexKnnQueries* index_reply);

/* Merge the K-NN answer from all bucket servers. Sorts the
   answer for each query from all buckets in terms of the
   distances from the query point. Finds the "K" smallest distances,
   after first removing the duplicates by performing a union.
 **A.S TODO: Make this O(nlogn), currently O(n^2).
In: thread arguments from all threads that communicated with 
corresponding bucket servers, query batch, number of queries,
dimension of each query, number of bucket servers,
number of nearest neighbors to be computed.
Out: the "K" nearest neighbors for each query in the
query batch.*/
void Merge(const struct ThreadArgs* thread_args, 
        const MultiplePoints &dataset,
        const MultiplePoints &queries_multiple_points,
        const unsigned int queries_size,
        const unsigned int query_dimensions,
        const unsigned int number_of_bucket_servers,
        const unsigned int number_of_nearest_neighbors,
        DistCalc* knn_answer,
        uint64_t* create_bucket_req_time,
        uint64_t* unpack_bucket_resp_time,
        uint64_t* unpack_bucket_req_time,
        uint64_t* calculate_knn_time,
        uint64_t* pack_bucket_resp_time);

/* Includes merging followed by packing. Used by the complete
   async code - when the async client thread receives all responses,
   this function is called so that the index_reply can be populated.
In: Map containing unique request and corresponding response meta data,
dataset in the form of multiple points,
queries,
query size, dimensions, number of bucket servers and neighbors to be computed.
Out: Index_reply - the data structure that must be populated and sent so that
the request can be marked as "finished" and the response can then be sent to the
load generator.*/
void MergeAndPack(const std::vector<ResponseData> &response_data,
        const MultiplePoints &dataset,
        const MultiplePoints &queries_multiple_points,
        const unsigned int queries_size,
        const unsigned int query_dimensions,
        const unsigned int number_of_bucket_servers,
        const unsigned int number_of_nearest_neighbors,
        loadgen_index::ResponseIndexKnnQueries* index_reply);

/* This function performs merge operation when there
   is async communication between index and bucket servers. There
   is a need for a different merge function because we merge
   only when all responses corresponding to a unique request id are
   received, So, the global map structure is read to accumulate all
   individual bucket responses collected so far.*/
void MergeFromResponseMap(const std::vector<ResponseData> &response_data,
        const MultiplePoints &dataset,
        const MultiplePoints &queries_multiple_points,
        const unsigned int queries_size,
        const unsigned int query_dimensions,
        const unsigned int number_of_bucket_servers,
        const unsigned int number_of_nearest_neighbors,
        DistCalc* knn_answer,
        uint64_t* create_bucket_req_time,
        uint64_t* unpack_bucket_resp_time,
        uint64_t* unpack_bucket_req_time,
        uint64_t* calculate_knn_time,
        uint64_t* pack_bucket_resp_time);

/* Print a Matrix of points - represents a collection of 
   points with 'd' dimensions.
In: matrix of ponts, number of rows, number of matrix columns.
Out: prints the matrix to terminal in a nice 2D pattern.*/
void PrintMatrix(const flann::Matrix<unsigned char> &matrix, 
        const unsigned int rows, 
        const unsigned int cols);

// Following list of functions apply only to the auto tuner.
void InitializeTMs(const int num_tms, 
        std::map<TMNames, TMConfig>* all_tms);
void InitializeAsyncTMs(const int num_tms,
        std::map<AsyncTMNames, TMConfig>* all_tms);

void InitializeFMSyncTMs(const int num_tms,
        std::map<FMSyncTMNames, TMConfig>* all_tms);

void InitializeFMAsyncTMs(const int num_tms,
        std::map<FMAsyncTMNames, TMConfig>* all_tms);
//void PrintPointIDs(std::vector<std::vector<uint32_t> >* point_ids_vec);
#endif //__LOADGEN_INDEX_SERVER_HELPER_H_INCLUDED__

