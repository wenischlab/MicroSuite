#include <cmath>
//#include <limits>
#include <omp.h>

#include "dist_calc.h"
#include "immintrin.h"
#include "utils.h"

#ifndef SIMD
#include "mkl_cblas.h"
#endif

// To use MKL, you can run these commands from a terminal.
// This is where the paths are on caen, they will obviously be different if you're
// on a different machine.
//
// export LD_LIBRARY_PATH=/usr/caen/intel-2017/mkl/lib/intel64:$LD_LIBRARY_PATH
// export LD_LIBRARY_PATH=/opt/caen/matlab-2016a/sys/os/glnxa64:$LD_LIBRARY_PATH
//
// Also, for some reason the first time you run a program with mkl it is very slow
// but all future runs are fast.
//
// Also, compiler flags:
//      icc -O3 --std=c++11 -mavx -mavx2 -pthread -fopenmp -mkl knn_test.cpp src/*.cpp service/timing.cc -o knn_test

/* All assertions on the critical path have been commented out. Remove comments
   if you only care about accuracy of result.*/

struct QueriesShardThreadArgs {
    int tid;
    MultiplePoints dataset;
    MultiplePoints queries;
    std::vector<std::vector<uint32_t>> point_ids_vec;
    /* This parameter is needed to refer to the private member variable
       knn_all_queries_ of the DistCalc class.*/
    DistCalc* knn_all_queries;
    unsigned number_of_nearest_neighbors;
    unsigned int num_procs;
};

struct DatasetShardThreadArgs {
    int tid;
    MultiplePoints dataset;
    Point query_point;
    std::vector<uint32_t> point_id_vec;
    unsigned int num_procs;
    std::vector<PointIDDistPair>* point_id_dist_pair_vec;
    DistCalc* this_obj;
};

void DistCalc::DistanceCalculation(const MultiplePoints &dataset, 
        const MultiplePoints &queries, 
        const std::vector<std::vector<uint32_t>> &point_ids_vec, 
        const unsigned number_of_nearest_neighbors,
        int num_cores)
{
    // A.S quick code start
    PointIDs point_ids_result_per_query(1, 0);
    knn_all_queries_.assign(1,
            point_ids_result_per_query);
    GetNN(dataset, queries.GetPointAtIndex(0), point_ids_vec[0], num_cores);
    // A.S quick code end
#if 0
    /* Initialize a data structure to hold knn for each query 
       and another data structure to hold knn for all queries.*/
    int num_points = std::min<int>(number_of_nearest_neighbors, dataset.GetSize());
    unsigned queries_size = queries.GetSize();
    PointIDs point_ids_result_per_query(num_points, 0);
    knn_all_queries_.assign(queries_size,
            point_ids_result_per_query);
    /* Get the number of concurrent threads that can be supported on the system,
       so that so many threads can be launched for parallel distance 
       computations.*/
    unsigned int num_procs = GetNumProcs();
    /* Restrict distance computations to this same thread if there aren't 
       sufficient number of queries. (helps while streaming instead of
       batching queries).*/
    // if (queries_size <= num_procs) {
    CustomPriorityQueue knn_priority_queue;

    // Writing some quick code for 1 query and 1 NN
    if ((number_of_nearest_neighbors == 1) && (queries_size == 1)) {
        GetNN(dataset, queries.GetPointAtIndex(0), point_ids_vec[0], num_cores);
    } else {

        // Following code holds good for multiple queries, and multiple NNs.
        // Iterate through all queries.
        for(int q = 0; q < queries_size; q++)
        {
            CalculateShardedKnn(dataset,
                    queries.GetPointAtIndex(q), 
                    point_ids_vec.at(q), 
                    number_of_nearest_neighbors,
                    &knn_priority_queue);
            /* Convert priority queue into a vector of points &
               Clear priority queue. */
            int idx = 0;
            while(!knn_priority_queue.IsEmpty())
            {
                point_ids_result_per_query[idx] = knn_priority_queue.GetTopPointID();
                idx++;
                knn_priority_queue.RemoveTopElement();
            }
            // The priority queue must now be empty.
            //assert(knn_priority_queue.GetSize() == 0);
            // Push the answer vector into the final result vector.
            knn_all_queries_[q] = point_ids_result_per_query;
        }
    }
        /* Otherwise queries are in a batch and distance computations
           must therefore be sharded across threads.*/
        // } else {
        //     CreateThreadsShardingQueries(&dataset, 
        //             &queries, 
        //             number_of_nearest_neighbors,
        //             num_procs,
        //             point_ids_vec);
        // }
        // Invariant: size of resultant vector must be size of query vector.
        //assert(knn_all_queries_.size() == queries_size);
#endif
}

void DistCalc::GetNN(const MultiplePoints &dataset,
        const Point &query_point,
        const std::vector<uint32_t> &point_id_vec,
        const int num_cores)
{
    int num_ids = point_id_vec.size();
    if (num_ids == 0) {
        PointIDs point_ids_result_per_query(1, 0);
        point_ids_result_per_query[0] = 0;
        knn_all_queries_[0] = point_ids_result_per_query;
        return;
    }
    /* Quick ret for the common case of one buckt srv and one NN.*/
    if ( (dataset.GetSize() == 1) && (num_ids == 1) ) {
        PointIDs point_ids_result_per_query(1, 0);
        point_ids_result_per_query[0] = point_id_vec[0];
        knn_all_queries_[0] = point_ids_result_per_query;
        return;
    }

    std::vector<float> min_dists(num_ids, 0.0);
    for(int i = 0; i < num_ids; i++)
    {
        min_dists[i] = EuclideanDistance(query_point, dataset.GetPointAtIndex(point_id_vec[i]));
    }


#if 0
    int paralellization = std::min(num_ids, num_cores);
    int tid = -1;
    int stride = num_ids/paralellization;
    std::vector<float> min_dists(num_ids, 0.0);
#pragma omp parallel num_threads(paralellization) private(paralellization, tid)
    {
        tid = omp_get_thread_num();
        int start_index = tid * stride;
        int end_index = (tid + 1) * stride;
        if (tid == (paralellization - 1)) {
            end_index = point_id_vec.size();
        }
        float euc_dist = 0.0;

#pragma omp parallel for
        for(int i = start_index; i < end_index; i++)
        {
            euc_dist = EuclideanDistance(query_point, dataset.GetPointAtIndex(point_id_vec[i]));
            min_dists[i] = euc_dist;
        }
    }
#endif

    int min_index = distance(min_dists.begin(), min_element(min_dists.begin(),min_dists.end()));

    PointIDs point_ids_result_per_query(1, 0);
    // The answer is the point id at the location of the min
    point_ids_result_per_query[0] = point_id_vec[min_index];
    knn_all_queries_[0] = point_ids_result_per_query;

}


void DistCalc::CreateThreadsShardingQueries(const MultiplePoints* dataset, 
        const MultiplePoints* queries,
        const unsigned int number_of_nearest_neighbors, 
        const unsigned int num_procs,
        const std::vector<std::vector<uint32_t>> &point_ids_vec)
{
    pthread_t threads[num_procs];
    struct QueriesShardThreadArgs* thread_args = new struct QueriesShardThreadArgs[num_procs];
    // Launch num_procs number of threads.
    for(int i = 0; i < num_procs; i++)
    {
        thread_args[i].tid = i;
        thread_args[i].dataset = *dataset;
        thread_args[i].queries = *queries;
        thread_args[i].number_of_nearest_neighbors = number_of_nearest_neighbors;
        thread_args[i].num_procs = num_procs;
        thread_args[i].point_ids_vec = point_ids_vec;
        /* K-NN answers for each query is filled up to the corresponding
           location of knn_all_queries_ by each worker thread. Hence, we need
           a pointer to the DistCalc object.*/
        thread_args[i].knn_all_queries = this;
        CHECK(!(pthread_create(&threads[i],
                        NULL,
                        &QueriesShardedDistanceCalculation,
                        (void*) &(thread_args[i]))),
                "ERROR: Unable to create thread to distance calculations in order to compute K-NN\n");
    }
    //This allows all threads to spawn before waiting for them to finish
    for(int i = 0; i < num_procs; i++){
        CHECK(!(pthread_join(threads[i],
                        NULL)),
                "ERROR: pthread_join failed while waiting for threads that launched ShardedDistanceComputation to complete\n");
    }
}

void* QueriesShardedDistanceCalculation(void* parameter)
{
    struct QueriesShardThreadArgs* thread_args = (QueriesShardThreadArgs*) parameter;
    PointIDs point_ids_result_per_query;
    unsigned queries_size = thread_args->queries.GetSize();
    // Create a priority queue.
    CustomPriorityQueue knn_priority_queue;
    int offset = (queries_size/thread_args->num_procs);
    int begin = thread_args->tid * offset;
    int end;
    /* It's possible for the queries to not be completely divisible by the 
       number of processors. In this case, have the last thread take care of 
       the left-over queries.*/  
    if(thread_args->tid == thread_args->num_procs - 1) {
        end = queries_size - 1;
    } else {
        end = ((thread_args->tid + 1) * offset) - 1;
    }
    for(int q = begin; q <= end; q++)
    {
        thread_args->knn_all_queries->CalculateKnn(thread_args->dataset, 
                thread_args->queries.GetPointAtIndex(q), 
                thread_args->point_ids_vec.at(q), 
                thread_args->number_of_nearest_neighbors,
                &knn_priority_queue);
        /* Convert priority queue into a vector of points &
           Clear priority queue. */
        while(!knn_priority_queue.IsEmpty())
        {
            point_ids_result_per_query.push_back(knn_priority_queue.GetTopPointID());
            knn_priority_queue.RemoveTopElement();
        }
        // The priority queue must now be empty.
        //assert(knn_priority_queue.GetSize() == 0);

        /* Push the answer vector into the 
           final result vector (to which we have a pointer).*/
        thread_args->knn_all_queries->AddKnnAnswer(point_ids_result_per_query, q);
        point_ids_result_per_query.clear();
    }
}

void DistCalc::CalculateKnn(const MultiplePoints &dataset, 
        const Point &query_point, 
        const std::vector<uint32_t> &point_id_vec, 
        const unsigned number_of_nearest_neighbors,
        CustomPriorityQueue* knn_priority_queue)
{
    float distance = 0.0;
    PointIDDistPair point_id_dist_pair;
    Point dataset_point;
    int num_point_ids = point_id_vec.size();
    // Iterate through the set of point IDs.
    for(int d = 0; d < num_point_ids; d++)
    {
        dataset_point = dataset.GetPointAtIndex(point_id_vec[d]);
        // Calculate distance between dataset point at point ID (candidate) and the query point.
        distance = EuclideanDistance(query_point, dataset_point);
        // Make a pair of the dataset point and its corresponding distance.
        point_id_dist_pair = std::make_pair(point_id_vec[d], distance);
        // Check if there's an empty spot in the priority queue.
        if (knn_priority_queue->GetSize() < number_of_nearest_neighbors) {
            // Populate the priority queue.
            knn_priority_queue->AddToPriorityQueue(point_id_dist_pair);
        } else {
            // Compare current distance with the largest distance in the queue.
            if (knn_priority_queue->GetTopDistance() > distance) {
                // Insert this distance in the queue and remove previous value.
                knn_priority_queue->RemoveTopElement();
                knn_priority_queue->AddToPriorityQueue(point_id_dist_pair);
            }
        }
        // Invariant: Once populated, the size of priority queue <= K.
        //assert(knn_priority_queue->GetSize() <= number_of_nearest_neighbors);
    }
}

void DistCalc::CalculateShardedKnn(const MultiplePoints &dataset, 
        const Point &query_point, 
        const std::vector<uint32_t> &point_id_vec, 
        const unsigned number_of_nearest_neighbors,
        CustomPriorityQueue* knn_priority_queue)
{
    unsigned int num_point_id = point_id_vec.size();
    float distance = 0.0;
    Point dataset_point = Point();
    PointIDDistPair point_id_dist_pair = std::make_pair(1, distance);
    std::vector<PointIDDistPair> point_id_dist_pair_vec(num_point_id, 
            point_id_dist_pair);
    int num_procs = GetNumProcs();
    CHECK((num_procs > 0), "ERROR: Number of available processors cannot be 0\n");
    /*Because this function gets called only when queries are streamed,
      it is good to parallelize by sharding distance calculations.*/
#pragma omp parallel num_threads(num_procs - 1)
    {
#pragma omp for private(distance) private(dataset_point)
        for(int p = 0; p < num_point_id; p++)
        {
            dataset_point = dataset.GetPointAtIndex(point_id_vec[p]);
            float distance = EuclideanDistance(query_point, dataset_point);
            point_id_dist_pair_vec[p] = std::make_pair(point_id_vec[p], distance);
        }
    }

    for(int i = 0; i < num_point_id; i++)
    {
        point_id_dist_pair = point_id_dist_pair_vec[i];
        distance = std::get<1>(point_id_dist_pair);
        // Check if there's an empty spot in the priority queue.
        if (knn_priority_queue->GetSize() < number_of_nearest_neighbors) {
            // Populate the priority queue.
            knn_priority_queue->AddToPriorityQueue(point_id_dist_pair);
        } else {
            // Compare current distance with the largest distance in the queue.
            if (knn_priority_queue->GetTopDistance() > distance) {
                // Insert this distance in the queue and remove previous value.
                knn_priority_queue->RemoveTopElement();
                knn_priority_queue->AddToPriorityQueue(point_id_dist_pair);
            }
        }
        // Invariant: Once populated, the size of priority queue <= K.
        assert(knn_priority_queue->GetSize() <= number_of_nearest_neighbors);
    }
}

void DistCalc::AddKnnAnswer(PointIDs& answer_curr_query, 
        const unsigned index)
{
    knn_all_queries_[index] = answer_curr_query;
}

float DistCalc::EuclideanDistance(const Point &query, 
        const Point &dataset_point) const
{
    unsigned int dimensions = dataset_point.GetSize();
    /* Invariant: dimensions must be the same to calaculate euclidean distance.
       Commenting invariant to gain some time.*/
    //assert(query.GetSize() == dimensions);
#ifndef SIMD
    Point dataset_tmp = dataset_point;
    cblas_saxpy(2048, -1, (float *) &query.point_[0], 1, (float*) &dataset_tmp.point_[0], 1);
    return cblas_snrm2(2048, &dataset_tmp.point_[0], 1);

#else
    float sum = 0.0;
    if(dimensions >= 16)
    {
        __m256 d_point, q_point, sub, square;
        for(int i = 0; i < (dimensions - 7); i+=8)
        {
            d_point = _mm256_set_ps(dataset_point.GetValueAtIndex(i+7), 
                    dataset_point.GetValueAtIndex(i+6),
                    dataset_point.GetValueAtIndex(i+5),
                    dataset_point.GetValueAtIndex(i+4),
                    dataset_point.GetValueAtIndex(i+3),
                    dataset_point.GetValueAtIndex(i+2),
                    dataset_point.GetValueAtIndex(i+1),
                    dataset_point.GetValueAtIndex(i));
            q_point = _mm256_set_ps(query.GetValueAtIndex(i+7),
                    query.GetValueAtIndex(i+6),
                    query.GetValueAtIndex(i+5),
                    query.GetValueAtIndex(i+4),
                    query.GetValueAtIndex(i+3),
                    query.GetValueAtIndex(i+2),
                    query.GetValueAtIndex(i+1),
                    query.GetValueAtIndex(i));
            sub = _mm256_sub_ps(d_point, q_point);
            square = _mm256_mul_ps(sub, sub);
            const __m128 x128 = _mm_add_ps(_mm256_extractf128_ps(square, 1), _mm256_castps256_ps128(square));
            const __m128 x64 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
            const __m128 x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
            sum += _mm_cvtss_f32(x32);
        }
    } else {
        sum = 0.0;
        float per_dim_difference = 0.0;
        for(int i = 0; i < dimensions; i++)
        {
            per_dim_difference = (dataset_point.GetValueAtIndex(i) - query.GetValueAtIndex(i));
            sum += per_dim_difference*per_dim_difference;
        }
    }
    // CHECK((isinf(sum) == 0), "ERROR: Euclidean Distance- Sum reaches infinity\n");
    // CHECK((sum >= 0.0), "ERROR: Distance between two points is complex\n");
    return sqrt(sum);
#endif
}

void DistCalc::Initialize(const int size, 
        const PointIDs &point_ids)
{
    knn_all_queries_.assign(size, point_ids);
}

unsigned DistCalc::GetSize() const
{
    return knn_all_queries_.size();
}

PointIDs DistCalc::GetValueAtIndex(const int index) const
{
    return knn_all_queries_[index];
}

void DistCalc::AddValueToIndex(const int index, const PointIDs &value)
{
    knn_all_queries_[index] = value;
}

void DistCalc::AddValueToBack(const PointIDs &value)
{
    knn_all_queries_.push_back(value);
}
