#include "client_helper.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using intersection::IntersectionRequest;
using intersection::UtilRequest;
using intersection::TimingDataInMicro;
using intersection::UtilResponse;
using intersection::IntersectionResponse;

void CreateIntersectionServiceRequest(const uint32_t intersection_server_id,
        const bool util_present,
        intersection::IntersectionRequest* request_to_intersection_srv)
{
    request_to_intersection_srv->set_intersection_server_id(intersection_server_id);
    request_to_intersection_srv->mutable_util_request()->set_util_request(util_present);
}

void UnpackIntersectionServiceResponse(const IntersectionResponse &reply, 
        std::vector<Docids>* posting_list,
        IntersectionSrvTimingInfo* intersection_srv_timing_info,
        IntersectionSrvUtil* intersection_srv_util)
{
    Docids num_ids = reply.doc_ids_size();
    posting_list = new std::vector<Docids>();
    for(int i = 0; i < num_ids; i++) {
        posting_list->emplace_back(reply.doc_ids(i));
    }
    UnpackTimingInfo(reply, intersection_srv_timing_info);
    UnpackUtilInfo(reply, intersection_srv_util);
}

void UnpackTimingInfo(const IntersectionResponse &reply,
        IntersectionSrvTimingInfo* intersection_srv_timing_info)
{
    intersection_srv_timing_info->unpack_intersection_srv_req_time = reply.timing_data_in_micro().unpack_intersection_srv_req_time_in_micro();
    intersection_srv_timing_info->calculate_intersection_time = reply.timing_data_in_micro().calculate_intersection_time_in_micro();
    intersection_srv_timing_info->pack_intersection_srv_resp_time = reply.timing_data_in_micro().pack_intersection_srv_resp_time_in_micro();
    intersection_srv_timing_info->cpu_util_intersection_srv = reply.timing_data_in_micro().cpu_util();
}

void UnpackUtilInfo(const IntersectionResponse &reply,
        IntersectionSrvUtil* intersection_srv_util)
{
    intersection_srv_util->util_present = reply.util_response().util_present();
    intersection_srv_util->user_time = reply.util_response().user_time();
    intersection_srv_util->system_time = reply.util_response().system_time();
    intersection_srv_util->io_time = reply.util_response().io_time();
    intersection_srv_util->idle_time = reply.util_response().idle_time();
}


#if 0

BucketClientCommandLineArgs* ParseBucketClientCommandLine(const int &argc, 
        char** argv)
{
    struct BucketClientCommandLineArgs* bucket_client_command_line_args = new struct BucketClientCommandLineArgs(); 
    if (argc == 2) {
        try
        {
            bucket_client_command_line_args->number_of_nearest_neighbors = std::stoul(argv[1], nullptr, 0);
        }
        catch (...)
        {
            CHECK(false, "Enter a valid number of nearest neighbors to be computed");
        }
    } else {
        CHECK(false, "Format: ./<test-program> <number of nearest neighbors to be computed");
    }

    return bucket_client_command_line_args;
}

void ReadPointIDsFromFile(const std::string &point_ids_file_name, 
        std::vector<std::vector<uint32_t> >* point_ids)
{
    std::ifstream point_ids_file(point_ids_file_name);
    std::vector<uint32_t> point_id;
    size_t size;
    // If file does not exist, error out.
    if(!point_ids_file.good()) {
        CHECK(false, "Cannot create point IDs from file because file does not exist\n");
    }
    std::string line;
    // Process each file line.
    for(int i = 0; std::getline(point_ids_file, line); i++)
    {
        std::istringstream buffer(line);
        std::istream_iterator<std::string> begin(buffer), end;
        std::vector<std::string> tokens(begin, end);
        for(auto& s: tokens)
        {
            try
            {
                point_id.emplace_back(static_cast<uint32_t>(std::stoi(s, &size)));
            }
            catch(...)
            {
                CHECK(false, "ERROR: Point IDs must be in the form of unsigned integers in the file.");
            }
        }
        point_ids->push_back(std::move(point_id));
        point_id.clear();
    }

}

void CreateBucketServiceRequest(const MultiplePoints &queries,
        const std::vector<std::vector<uint32_t>> &point_ids,
        const unsigned queries_size,
        const unsigned number_of_nearest_neighbors,
        const int dimension,
        const uint32_t bucket_server_id,
        const int shard_size,
        const bool util_present,
        NearestNeighborRequest* request)
{
#if 0
    // Add each query to the query list.
    for(int i = 0; i < queries_size; i++)
    {
        DataPoint* single_query_point = request->add_queries();
        PointIdList* point_id_single_query = request->add_maybe_neighbor_list();
        // Add individual dimensions of a query.
        int num_point_ids = point_ids[i].size();
        if (dimension > num_point_ids) {
            for(int j = 0; j < dimension; j++)
            {
                single_query_point->add_data_point(queries.GetPointAtIndex(i).GetIntersectionResponseAtIndex(j));
                if (j < num_point_ids) {
                    point_id_single_query->add_point_id(point_ids[i][j]);
                }
            }
        } else {
            for(int j = 0; j < num_point_ids; j++)
            {               
                point_id_single_query->add_point_id(point_ids[i][j]);
                if (j < dimension) {
                    single_query_point->add_data_point(queries.GetPointAtIndex(i).GetIntersectionResponseAtIndex(j));
                }
            }                                               
        }
    }
#endif
    for(int i = 0; i < queries_size; i++)
    {
        PointIdList* point_id_single_query = request->add_maybe_neighbor_list();
        int num_point_ids = point_ids[i].size();
        for(int j = 0; j < num_point_ids; j++)
        {
            point_id_single_query->add_point_id(point_ids[i][j]);
        }
    }

    // Add number of nearest neighbors to the request.
    request->set_requested_neighbor_count(number_of_nearest_neighbors);

    /* Add the bucket server number, so that dataset points 
       can be searched based on the shard that a bucket holds*/
    request->set_bucket_server_id(bucket_server_id);
    request->set_shard_size(shard_size);

    /* Send info of whether the bucket needs to return
       its utilization info or not.*/
    request->mutable_util_request()->set_util_request(util_present);
}


void UnpackBucketServiceResponse(const NearestNeighborResponse &reply, 
        const unsigned number_of_nearest_neighbors,
        DistCalc* knn_answer, 
        BucketTimingInfo* bucket_timing_info,
        BucketUtil* bucket_util)
{
    uint32_t id_value = 0;
    PointIDs point_ids_per_query;
    for(int i = 0; i < reply.neighbor_ids_size(); i++)
    {
        point_ids_per_query.assign(reply.neighbor_ids(i).point_id_size(), 0);
        for(int j = 0; j < reply.neighbor_ids(i).point_id_size(); j++)
        {
            id_value = reply.neighbor_ids(i).point_id(j);
            point_ids_per_query[j] = id_value;
        }
        knn_answer->AddIntersectionResponseToBack(point_ids_per_query);
        point_ids_per_query.clear();
    }
    UnpackTimingInfo(reply, bucket_timing_info);
    UnpackUtilInfo(reply, bucket_util);
}

void UnpackTimingInfo(const NearestNeighborResponse &reply,
        BucketTimingInfo* bucket_timing_info)
{
    bucket_timing_info->unpack_bucket_req_time = reply.timing_data_in_micro().unpack_bucket_req_time_in_micro();
    bucket_timing_info->calculate_knn_time = reply.timing_data_in_micro().calculate_knn_time_in_micro();
    bucket_timing_info->pack_bucket_resp_time = reply.timing_data_in_micro().pack_bucket_resp_time_in_micro();
    bucket_timing_info->cpu_util_bucket = reply.timing_data_in_micro().cpu_util();
}

void UnpackUtilInfo(const NearestNeighborResponse &reply,
        BucketUtil* bucket_util)
{
    bucket_util->util_present = reply.util_response().util_present();
    bucket_util->user_time = reply.util_response().user_time();
    bucket_util->system_time = reply.util_response().system_time();
    bucket_util->io_time = reply.util_response().io_time();
    bucket_util->idle_time = reply.util_response().idle_time();
}

void WriteKNNToFile(const std::string &knn_file_name, const DistCalc &knn_answer)
{
    std::ofstream knn_file(knn_file_name);
    // Error out if file could not be opened.
    CHECK(knn_file.good(), "ERROR: Bucket client could not open file to write KNN answer.\n");
    for(int i = 0 ; i < knn_answer.GetSize(); i++)
    {
        // Insert blank line to indicate knn for next query.
        //knn_file << "\n";
        for(int j = 0; j < knn_answer.GetIntersectionResponseAtIndex(i).size(); j++)
        {
            knn_file << knn_answer.GetIntersectionResponseAtIndex(i).at(j) << " ";
        }
        knn_file << "\n";
    }
    knn_file.close();
}

void PrintKNNForAllQueries(const DistCalc &knn_answer)
{
    for(int i = 0; i < knn_answer.GetSize(); i++)
    {
        std::cout << "Q" << i << ": " << std::endl;
        for(uint32_t j = 0; j < knn_answer.GetIntersectionResponseAtIndex(i).size(); j++)
        {
            std::cout << static_cast<uint32_t> (knn_answer.GetIntersectionResponseAtIndex(i).at(j)) << " ";
        }
        std::cout << std::endl;
    }

}

void PrintPointIDs(const std::vector<std::vector<uint32_t> > &point_ids)
{

    int count = 0;
    for(const std::vector<uint32_t>& i : point_ids)
    {
        std::cout << "Q" << count << " point IDs" << std::endl;
        count++;
        for(const uint32_t& j : i)
        {
            std::cout << static_cast<uint32_t> (j) << " ";
        }
        std::cout << std::endl;
    }
}

void CreatePointsFromFile(const std::string &file_name, 
        MultiplePoints* multiple_points)
{
    multiple_points->CreateMultiplePoints(file_name);
}
#endif

