#ifndef __SERVER_HELPER_H_INCLUDED__
#define __SERVER_HELPER_H_INCLUDED__

#include "bucket_service/src/dist_calc.h"
#include "protoc_files/bucket.grpc.pb.h"

/* Create a dataset, given a binary file containing float values.
   Dimensions of all points are 2048.
In: name of the dataset file
Out: dataset in the form of MultiplePoints.*/
void CreateDatasetFromBinaryFile(const std::string &file_name, 
        const int bucket_server_num,
        const int num_bucket_servers,
        MultiplePoints* dataset);

/* Unpack protobuf message request from the index into a vector of query 
   points, set of point IDs for each query, and bucket server ID.
In: protobuf message request.
Out: vector of queries, set of point IDs for each query, bucket server ID,
shard size*/ 
void UnpackBucketServiceRequest(const bucket::NearestNeighborRequest &request, 
        const MultiplePoints &dataset,
        MultiplePoints* queries, 
        std::vector<std::vector<uint32_t>>* point_ids_vec,
        uint32_t* bucket_server_id,
        uint32_t* shard_size);

/* Same as above function except that a piggy back message gets
   added to the bucket reply.*/
void UnpackBucketServiceRequestAsync(const bucket::NearestNeighborRequest &request,
        const MultiplePoints &dataset,
        MultiplePoints* queries,
        std::vector<std::vector<uint32_t>>* point_ids_vec,
        uint32_t* bucket_server_id,
        uint32_t* shard_size,
        bucket::NearestNeighborResponse* reply);
/* Unpack protobuf message request from the index into a vector of query 
   points. This functionality of this function is a subset of the above 
   function.
In: protobuf message request.
Out: vector of query points.*/
void UnpackQueries(const bucket::NearestNeighborRequest &request, 
        const MultiplePoints &dataset,
        MultiplePoints* queries);

/* Same as above function but populates a piggy back message 
   to the server.*/
void UnpackQueriesAsync(const bucket::NearestNeighborRequest &request,
        const MultiplePoints &dataset,
        MultiplePoints* queries,
        bucket::NearestNeighborResponse* reply);
/* Unpack protobuf message request from the index into a 
   set of point IDs for each query. This functionality of this function is a 
   subset of the above function.
In: protobuf message request, bucket server ID, shard size.
Out: set of point IDs for each query.
Note: Bucket server ID and shard size are required inputs, so that the bucket
knows which corresponding piece of the dataset it must bring into memory.*/
void UnpackPointIDs(const bucket::NearestNeighborRequest &request, 
        const uint32_t bucket_server_id,
        const int shard_size,
        std::vector<std::vector<uint32_t>>* point_ids_vec);

/* Remove duplicate point IDs. It is possible for duplicate point IDs to be
   present even with a single hash table because of the use of multiple
   xor masks. A design choice would be to store list of sorted vectors in each
   bucket so that each list gets sent a corresponding bucket server. But, this
   alters the underlying data structure way too much. TODO: must thing about this.
In: Pointer to the vector that needs to be modified
Out: Modified unique vector.*/
void RemoveDuplicatePointIDs(std::vector<std::vector<uint32_t> > &point_ids_vec);

/* Given a batch of query points, the dataset shard, the point IDs
   for which distance calculations must be performed, the number of
   nearest neighbors to be computed, return the k-nn for each query.
In: vector of query points, the dataset shard corresponding to
this bucket server, set of point IDs for each query, and the number
of nearest neighbors that must be computed.
Out: K-NN for each query point.*/ 
void CalculateKNN(const MultiplePoints &queries, 
        const MultiplePoints &dataset, 
        const std::vector<std::vector<uint32_t>> &point_ids_vec, 
        const uint32_t number_of_nearest_neighbors,
        const int num_cores,
        DistCalc* knn_answer);
/* Pack the k-NN for each query point into a protobuf reply message
   so that we can ship it off to the index server.
In: K-NN for each query point.
Out: protobuf reply message to the index server. */
void PackBucketServiceResponse(const DistCalc &knn_answer, 
        const uint32_t bucket_server_id,
        const uint32_t shard_size,
        bucket::NearestNeighborResponse* reply);

/* Creates a set of points from the text file name provided.
   If there exceptions during file creation, program exits.
In: text file name containing data points.
Out: vector of points.*/ 
void CreatePointsFromFile(const std::string &file_name, 
        MultiplePoints* multiple_points);

#endif //__SERVER_HELPER_H_INCLUDED__
