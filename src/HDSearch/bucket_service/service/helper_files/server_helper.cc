#include "server_helper.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using bucket::DataPoint;
using bucket::MultipleDataPoints;
using bucket::PointIdList;
using bucket::NearestNeighborRequest;
using bucket::TimingDataInMicro;
using bucket::NearestNeighborResponse;
using bucket::DistanceService;

void CreateDatasetFromBinaryFile(const std::string &file_name, 
        const int bucket_server_num,
        const int num_bucket_servers, 
        MultiplePoints* dataset)
{
    int dataset_dimensions = 2048;
    std::cout << "Assuming 2048 dimensions\n";
    FILE *dataset_binary = fopen(file_name.c_str(), "r");
    if(!dataset_binary)
    {
        CHECK(false, "ERROR: Could not open dataset file\n");
    }
    /* Get the size of the file, so that we can initialize number
       of points, given 2048 dimensions.*/
    std::ifstream stream_to_get_size(file_name, std::ios::binary | std::ios::in);
    stream_to_get_size.seekg(0, std::ios::end);
    long size = stream_to_get_size.tellg();
    long dataset_size = (size/sizeof(float))/dataset_dimensions;
    CHECK((dataset_size >= 0), "ERROR: Negative number of points in the dataset\n");
    /* Initialize dataset to contain 0s, 
       this is so that we can directly index every point/value after this.*/
    Point p(dataset_dimensions, 0.0);
    dataset->Resize(dataset_size, p);

    //Read each point (dimensions = 2048) and create a set of multiple points.
    float values[dataset_dimensions];
    long shard_size = dataset_size/num_bucket_servers;
    long start_index = bucket_server_num * shard_size;
    long end_index = (bucket_server_num + 1) * shard_size;
    /* If this is the last bucket server, get it to cover all points til the end,
       if note - just focus on it's shard. This is to prevent points from being ignored
       when the data set does not shard evenly.*/
    if (bucket_server_num == (num_bucket_servers - 1)) {
        end_index = dataset_size;
    }

    for(long i = start_index; i < end_index; i++)
    {
        if(fread(values, sizeof(float), dataset_dimensions, dataset_binary) == dataset_dimensions)
        {
            p.CreatePointFromFloatArray(values, dataset_dimensions);
            dataset->SetPoint(i, p);
        } else {
            break;
        }
    }
}

void UnpackBucketServiceRequest(const NearestNeighborRequest &request, 
        const MultiplePoints &dataset,
        MultiplePoints* queries, 
        std::vector<std::vector<uint32_t>>* point_ids_vec, 
        uint32_t* bucket_server_id,
        uint32_t* shard_size)
{

    UnpackQueries(request, dataset, queries);
    *bucket_server_id = (uint32_t)request.bucket_server_id();
    *shard_size = (int)request.shard_size();
    UnpackPointIDs(request, *bucket_server_id, *shard_size, point_ids_vec);
}

void UnpackBucketServiceRequestAsync(const bucket::NearestNeighborRequest &request,
        const MultiplePoints &dataset,
        MultiplePoints* queries,
        std::vector<std::vector<uint32_t>>* point_ids_vec,
        uint32_t* bucket_server_id,
        uint32_t* shard_size,
        bucket::NearestNeighborResponse* reply)
{
    UnpackQueriesAsync(request, dataset, queries, reply);
    *bucket_server_id = (uint32_t)request.bucket_server_id();
    *shard_size = (int)request.shard_size();
    UnpackPointIDs(request, *bucket_server_id, *shard_size, point_ids_vec);
}

void UnpackQueries(const bucket::NearestNeighborRequest &request, 
        const MultiplePoints &dataset,
        MultiplePoints* queries)
{
    Point p(dataset.GetPointAtIndex(0).GetSize(), 0.0);
    uint64_t value = 0;
    for(int i = 0; i < request.queries_size(); i++)
    {
        value = request.queries(i);
        p = dataset.GetPointAtIndex(value);
        queries->SetPoint(i, p);
    }
}

void UnpackQueriesAsync(const bucket::NearestNeighborRequest &request,
        const MultiplePoints &dataset,
        MultiplePoints* queries,
        bucket::NearestNeighborResponse* reply)
{
    Point p(dataset.GetPointAtIndex(0).GetSize(), 0.0);
    uint64_t value = 0;
    for(int i = 0; i < request.queries_size(); i++)
    {
        value = request.queries(i);
        reply->add_queries(value);
        p = dataset.GetPointAtIndex(value);
        queries->SetPoint(i, p);
    }

}

void UnpackPointIDs(const NearestNeighborRequest &request, 
        const uint32_t bucket_server_id,
        const int shard_size,
        std::vector<std::vector<uint32_t>>* point_ids_vec)
{

    uint32_t id_value;
    std::vector<uint32_t> point_ids_per_query;

    // UnPacking Point IDs.
    int num_queries = request.maybe_neighbor_list_size();
    int num_ids = 0;
    for(int i = 0; i < num_queries; i++)
    {
        num_ids = request.maybe_neighbor_list(i).point_id_size();
        for(int j = 0; j < num_ids; j++)
        {
            id_value = request.maybe_neighbor_list(i).point_id(j);
            //id_value = id_value - (bucket_server_id * shard_size);
            point_ids_per_query.push_back(id_value);
        }
        point_ids_vec->push_back(point_ids_per_query);
        if (num_queries > 1) {
            point_ids_per_query.clear();
        }
    }
}

void RemoveDuplicatePointIDs(std::vector<std::vector<uint32_t> > &point_ids_vec)
{
    uint32_t point_ids_size = point_ids_vec.size();
    for(int i = 0; i < point_ids_size; i++)
    {
        std::set<uint32_t> point_id_set(point_ids_vec[i].begin(), point_ids_vec[i].end());
        point_ids_vec[i].assign(point_id_set.begin(), point_id_set.end());
    }
}

void CalculateKNN(const MultiplePoints &queries, 
        const MultiplePoints &dataset, 
        const std::vector<std::vector<uint32_t>> &point_ids_vec, 
        const uint32_t number_of_nearest_neighbors,
        const int num_cores,
        DistCalc* knn_answer)
{
    knn_answer->DistanceCalculation(dataset, 
            queries, 
            point_ids_vec, 
            (unsigned)number_of_nearest_neighbors,
            num_cores);
}

void PackBucketServiceResponse(const DistCalc &knn_answer, 
        const uint32_t bucket_server_id,
        const uint32_t shard_size,
        NearestNeighborResponse* reply)
{
    int knn_answer_size = knn_answer.GetSize();
    int neighbor_ids_size = 0;
    for(int i = 0; i < knn_answer_size; i++)
    {
        PointIdList* knn = reply->add_neighbor_ids();
        neighbor_ids_size = knn_answer.GetValueAtIndex(i).size();
        for(int j = 0; j < neighbor_ids_size; j++)
        {
            knn->add_point_id((uint32_t)(knn_answer.GetValueAtIndex(i).at(j)));
            //knn->add_point_id((uint32_t)(knn_answer.GetValueAtIndex(i).at(j) + (bucket_server_id * shard_size)));
        }
    }
}

void CreatePointsFromFile(const std::string &file_name, 
        MultiplePoints* multiple_points)
{
    multiple_points->CreateMultiplePoints(file_name);
}
