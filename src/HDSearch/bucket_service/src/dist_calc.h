#ifndef __DIST_CALC_H_INCLUDED__
#define __DIST_CALC_H_INCLUDED__

#include <math.h>
#include <mutex>
#include <unistd.h>
#include "custom_priority_queue.h"

/* This is the function called by individual worker threads and 
   is therefore not a member of the DistCalc class.*/
void* QueriesShardedDistanceCalculation(void* parameter);

typedef std::vector<uint32_t> PointIDs;
class DistCalc 
{
    public:
        DistCalc() = default;
        // Calculates the k-nn points for all queries.
        void DistanceCalculation(const MultiplePoints &dataset, 
                const MultiplePoints &queries, 
                const std::vector<std::vector<uint32_t>> &point_ids_vec, 
                const unsigned number_of_nearest_neighbors,
                const int num_cores);

        /* Faster function when only one NN needs to be computed for
           one query.*/
        void GetNN(const MultiplePoints &dataset,
                const Point &query_point,
                const std::vector<uint32_t> &point_id_vec,
                const int num_cores);

        void CreateThreadsShardingQueries(const MultiplePoints* dataset,
                const MultiplePoints* queries,
                const unsigned int number_of_nearest_neighbors,
                const unsigned int num_procs,
                const std::vector<std::vector<uint32_t>> &point_ids_vec);
        // Calculates k-points between one query and all points in the dataset.
        void CalculateKnn(const MultiplePoints &dataset, 
                const Point &query_point, 
                const std::vector<uint32_t> &point_id_vec,
                const unsigned number_of_nearest_neighbors,
                CustomPriorityQueue *knn_priority_queue);
        void CalculateShardedKnn(const MultiplePoints &dataset,
                const Point &query_point,
                const std::vector<uint32_t> &point_id_vec,
                const unsigned number_of_nearest_neighbors,
                CustomPriorityQueue* knn_priority_queue);
        // Add the K-NN result to each query in the batch.
        void AddKnnAnswer(PointIDs& answer_curr_query, 
                const unsigned index);
        // Calculates euclidean distance between a query & dataset point.
        float EuclideanDistance(const Point &query, 
                const Point &dataset_point) const;
        // Initializes knn_all_queries_ with a given size & value.
        void Initialize(const int size, 
                const PointIDs &point_ids);
        // Returns size of knn_all_queries_.
        unsigned GetSize() const;
        // Returns the MultiplePoints at the given index of knn_all_queries_.
        PointIDs GetValueAtIndex(const int index) const;
        // Adds given value to given index of knn_all_queries_.
        void AddValueToIndex(const int index, const PointIDs &value);
        // Adds value to the back of the vector knn_all_queries_.
        void AddValueToBack(const PointIDs &value);
    private:
        // Data structure that holds k-nn (MultiplePoints) for all queries.
        std::vector<PointIDs> knn_all_queries_;
};
#endif //__DIST_CALC_H_INCLUDED__
