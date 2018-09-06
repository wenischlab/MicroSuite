/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <unordered_map>
#include "mid_tier_server_helper.h"

#define NODEBUG

IndexServerCommandLineArgs* ParseIndexServerCommandLine(const int argc, char** argv)
{
    struct IndexServerCommandLineArgs* index_server_command_line_args = new struct IndexServerCommandLineArgs();
    if (argc == 13) {
        try
        {
            index_server_command_line_args->num_hash_tables = std::stoul(argv[1], nullptr, 0);
            index_server_command_line_args->hash_table_key_length = std::stoul(argv[2], nullptr, 0);
            index_server_command_line_args->num_multi_probe_levels = std::stoul(argv[3], nullptr, 0);
            index_server_command_line_args->number_of_bucket_servers = std::stoul(argv[4], nullptr, 0);
            index_server_command_line_args->bucket_server_ips_file = argv[5];
            index_server_command_line_args->dataset_file_name = argv[6];
            index_server_command_line_args->mode = std::stoul(argv[7], nullptr, 0);
            index_server_command_line_args->ip = argv[8];
            index_server_command_line_args->network_poller_parallelism = std::stoul(argv[9], nullptr, 0);
            index_server_command_line_args->dispatch_parallelism = std::stoul(argv[10], nullptr, 0);
            index_server_command_line_args->number_of_async_response_threads = std::stoul(argv[11], nullptr, 0);
            index_server_command_line_args->get_profile_stats = std::stoul(argv[12], nullptr, 0);
        }
        catch (...)
        {
            CHECK(false, "Enter a valid number for num_hash_tables/hash_table_key_length/num_multi_probe_levels/number of bucket servers/file containing bucket server IPS/valid string for dataset path/ mode number/ index server IP address/ number of network poller threads/ number of dispatch threads/ number of async response threads/ get profile stats - this is either 0 or 1");
        }
    } else {
        CHECK(false, "Format: ./<loadgen_index_server> <num_hash_tables> <hash_table_key_length> <num_multi_probe_levels> <number_of_bucket_servers> <file containing bucket server IPs> <dataset file path> <mode number: 1 - read dataset from text file, 2 - binary file> <index server IP address> <number of network poller threads> <number of dispatch threads> <number of async response threads> <get profile stats>");
    }
    return index_server_command_line_args;
}

void GetBucketServerIPs(const std::string &bucket_server_ips_file,
        std::vector<std::string>* bucket_server_ips)
{
    std::ifstream file(bucket_server_ips_file);
    CHECK((file.good()), "ERROR: File containing bucket server IPs must exists\n");
    std::string line = "";
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buffer(line);
        std::istream_iterator<std::string> begin(buffer), end;
        std::vector<std::string> tokens(begin, end);
        /* Tokens must contain only one IP -
           Each IP address must be on a different line.*/
        CHECK((tokens.size() == 1), "ERROR: File must contain only one IP address per line\n");
        bucket_server_ips->push_back(tokens[0]);
    }
}

void BuildLshIndex(const flann::Matrix<unsigned char> &dataset,
        const unsigned int num_hash_tables,
        const unsigned int hash_table_key_length,
        const unsigned int num_multi_probe_levels, 
        flann::Index<flann::L2<unsigned char> >* lsh_index)
{
    lsh_index->initializeIndex(dataset, 
            flann::LshIndexParams(num_hash_tables, 
                hash_table_key_length, 
                num_multi_probe_levels));
    lsh_index->buildIndex();
}

void LoadLshIndexFromFile(const std::string &index_file_name,
        const flann::Matrix<unsigned char> &dataset,
        flann::Index<flann::L2<unsigned char> >* lsh_index)
{
    lsh_index->initializeIndex(dataset, 
            flann::SavedIndexParams(index_file_name));
}

void UnpackLoadgenServiceRequest(const loadgen_index::LoadGenRequest &load_gen_request,
        const MultiplePoints &dataset,
        const unsigned int queries_size,
        const unsigned int query_dimensions,
        flann::Matrix<unsigned char>* queries, 
        MultiplePoints* queries_multiple_points,
        uint64_t* query_id,
        bucket::NearestNeighborRequest* request_to_bucket)
{
    // UnPacking Queries.
    Point dataset_point(2048, 0.0);
    for(unsigned int i = 0; i < queries_size; i++)
    {
        *(query_id) = load_gen_request.query_id(i);
        request_to_bucket->add_queries(*(query_id));
        dataset_point = dataset.GetPointAtIndex(*(query_id));
        queries_multiple_points->SetPoint(i, dataset_point);

        for(unsigned int j = 0; j < query_dimensions; j++) {

            /* Casting float to unsigned because flann's LSH works 
               only for unsigned char.*/
            (*queries)[i][j] = static_cast<unsigned char>(dataset_point.GetValueAtIndex(j)*255);
        }
    }
}

flann::Matrix<unsigned char>* CreateDatasetFromTextFile(const std::string &file_name, 
        long* dataset_size, 
        unsigned int* dataset_dimensions)
{
    MultiplePoints dataset_multiple_points;
    dataset_multiple_points.CreateMultiplePoints(file_name);
    (*dataset_size) = dataset_multiple_points.GetSize();
    (*dataset_dimensions) = dataset_multiple_points.GetPointAtIndex(0).GetSize();

    // Define dataset Matrix.
    flann::Matrix<unsigned char> dataset(new unsigned char[(*dataset_size) * (*dataset_dimensions)], 
            (*dataset_size), 
            (*dataset_dimensions));
    for(unsigned int m = 0; m < (*dataset_size); m++)
    {
        for(unsigned int n = 0; n < (*dataset_dimensions); n++)
        {
            dataset[m][n] = static_cast<unsigned char>(dataset_multiple_points.GetPointAtIndex(m).GetValueAtIndex(n)*255);
        }
    }
    return &dataset;
}

void CreateDatasetFromBinaryFile(const std::string &file_name,
        long* dataset_size, 
        unsigned int* dataset_dimensions,
        flann::Matrix<unsigned char>* dataset)
{
    (*dataset_dimensions) = 2048;
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
    (*dataset_size) = (size/sizeof(float))/(*dataset_dimensions);
    CHECK(((*dataset_size) >= 0), "ERROR: Dataset cannot have negative number of points\n");
    CHECK(((*dataset_dimensions) >= 0), "ERROR: Number of dataset dimensions cannot be negative\n");

    //Read each individual float value and store it in the dataset matrix.
    float value;
    *dataset = flann::Matrix<unsigned char>(new unsigned char[(*dataset_size) * (*dataset_dimensions)],
            (*dataset_size),
            (*dataset_dimensions));
    /*flann::Matrix<unsigned char> dataset(new unsigned char[(*dataset_size) * (*dataset_dimensions)],
      (*dataset_size),
      (*dataset_dimensions));*/
    for(long m = 0; m < (*dataset_size); m++)
    {
        for(unsigned int n = 0; n < (*dataset_dimensions); n++)
        {
            if(fread((void*)(&value), sizeof(value), 1, dataset_binary) == 1)
            {
                (*dataset)[m][n] = static_cast<unsigned char>(value*255);
            } else {
                break;
            }
        }
    }
}

flann::Matrix<unsigned char>* CreateDatasetWithTagsFromBinaryFile(
        const std::string &file_name,
        long* dataset_size,
        unsigned int* dataset_dimensions,
        std::vector<Key>* keys)
{
    (*dataset_dimensions) = 2048;
    FILE *dataset_binary = fopen(file_name.c_str(), "r");
    if(!dataset_binary)
    {
        CHECK(false, "ERROR: Could not open dataset file\n");
    }
    /* Get the size of the file, so that we can initialize number
       of points, given 2048 dimensions.*/
    std::ifstream stream_to_get_size(file_name, std::ios::binary | std::ios::in);
    stream_to_get_size.seekg(0, std::ios::end);
    long long size = stream_to_get_size.tellg();
    long size_of_tags = 16;
    (*dataset_size) = size/( (sizeof(float)*(*dataset_dimensions)) + size_of_tags);
    CHECK(((*dataset_size) >= 0), "ERROR: Dataset cannot have negative number of points\n");
    CHECK(((*dataset_dimensions) >= 0), "ERROR: Number of dataset dimensions cannot be negative\n");

    //Read each individual float value and store it in the dataset matrix.
    float value = 0.0;
    Key* key_obj = new Key();
    keys->resize((*dataset_size), *key_obj);
    flann::Matrix<unsigned char> dataset(new unsigned char[(*dataset_size) * (*dataset_dimensions)],
            (*dataset_size),
            (*dataset_dimensions));
    for(int m = 0; m < (*dataset_size); m++)
    {
        for(unsigned int n = 0; n < (*dataset_dimensions) + 1; n++)
        {
            if (n == 0)
            {
                if (fread(key_obj, sizeof(Key), 1, dataset_binary) != 1) break;
                for(int i = 0; i < 16; i++) {
                    std::cout << key_obj->key[i];
                }
                std::cout << std::endl;
                keys->at(m) = *key_obj;
                continue;
            }
            if(fread((void*)(&value), sizeof(value), 1, dataset_binary) == 1)
            {
                dataset[m][n] = static_cast<unsigned char>(value*255);
            } else {
                break;
            }
        }
    }
    return &dataset;
}

void CreateMultiplePointsFromBinaryFile(const std::string &file_name, MultiplePoints* dataset)
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
    for(int i = 0; i < dataset_size; i++)
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


void ValidateDimensions(const unsigned int dataset_dimension, 
        const unsigned int point_dimension)
{
    // If the dimensions are unequal, exit with exception.
    CHECK((dataset_dimension == point_dimension), "Dimensions of all points (dataset & queries) must be equal");

}

bool CheckEmptyPointIDs(const std::vector<std::vector<uint32_t> > &point_ids_vec)
{
    for(uint32_t i = 0; i < point_ids_vec.size(); i++)
    {
        if (point_ids_vec.at(i).size() == 0) {
            return true;
        }
    }
    return false;
}

void PopulatePointIDs(std::vector<std::vector<uint32_t> >* point_ids_vec)
{
    for(uint32_t i = 0; i < point_ids_vec->size(); i++)
    {
        if (point_ids_vec->at(i).size() == 0) {
            point_ids_vec->at(i).push_back(0);
        }
    }
}

float PercentDataSent(const std::vector<std::vector<uint32_t> > &point_ids,
        const unsigned int queries_size,
        const unsigned int dataset_size)
{
    int points_sent = 0, total_points = queries_size * dataset_size;
    for(unsigned int i = 0; i < point_ids.size(); i++)
    {
        points_sent += point_ids[i].size();
    }
    return (points_sent*100.0)/total_points;
}

void PackIndexServiceResponse(const DistCalc &knn_answer, 
        ThreadArgs* thread_args,
        const unsigned int number_of_bucket_servers,
        loadgen_index::ResponseIndexKnnQueries* index_reply)
{
    int knn_answer_size = knn_answer.GetSize();
    int neighbor_ids_size = 0;
    for(int i = 0; i < knn_answer_size; i++)
    {
        loadgen_index::PointIds* point_id_single_query = index_reply->add_neighbor_ids();
        neighbor_ids_size = knn_answer.GetValueAtIndex(i).size();
        // Add individual Point IDs for current query.
        for(int j = 0; j < neighbor_ids_size; j++)
        {
            point_id_single_query->add_point_id((uint32_t)knn_answer.GetValueAtIndex(i).at(j));
        }
    }

    /* Pack util info for all bucket servers. We do not
       want the mean because we want to know how each bucket behaves i.e
       does one perform worse than the others / are they uniform? */
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        loadgen_index::Util* bucket_util = index_reply->mutable_util_response()->add_bucket_util();
        bucket_util->set_user_time(thread_args[i].bucket_util.user_time);
        bucket_util->set_system_time(thread_args[i].bucket_util.system_time);
        bucket_util->set_io_time(thread_args[i].bucket_util.io_time);
        bucket_util->set_idle_time(thread_args[i].bucket_util.idle_time);
    }
}

void PackIndexServiceResponseFromMap(const DistCalc &knn_answer,
        const std::vector<ResponseData> &response_data,
        const unsigned int number_of_bucket_servers,
        loadgen_index::ResponseIndexKnnQueries* index_reply)
{
    int knn_answer_size = knn_answer.GetSize();
    int neighbor_ids_size = 0;
    for(int i = 0; i < knn_answer_size; i++)
    {
        loadgen_index::PointIds* point_id_single_query = index_reply->add_neighbor_ids();
        neighbor_ids_size = knn_answer.GetValueAtIndex(i).size();
        // Add individual Point IDs for current query.
        for(int j = 0; j < neighbor_ids_size; j++)
        {
            point_id_single_query->add_point_id((uint32_t)knn_answer.GetValueAtIndex(i).at(j));
        }
    }

    /* Pack util info for all bucket servers. We do not
       want the mean because we want to know how each bucket behaves i.e
       does one perform worse than the others / are they uniform? */
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        loadgen_index::Util* bucket_util = index_reply->mutable_util_response()->add_bucket_util();
        bucket_util->set_user_time(response_data[i].bucket_util->user_time);
        bucket_util->set_system_time(response_data[i].bucket_util->system_time);
        bucket_util->set_io_time(response_data[i].bucket_util->io_time);
        bucket_util->set_idle_time(response_data[i].bucket_util->idle_time);
    }

}

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
        uint64_t* pack_bucket_resp_time)
{
    std::vector<std::vector<uint32_t>> knn_all_queries;
    std::vector<uint32_t> knn;
    for(unsigned int i = 0; i < queries_size; i++)
    {
        for(unsigned int j = 0; j < number_of_bucket_servers; j++)
        {
            /* Calculate the avergae time for all components from different
               bucket servers and send the mean as the final time.*/
            (*create_bucket_req_time) += thread_args[j].bucket_timing_info.create_bucket_request_time;
            (*unpack_bucket_resp_time) += thread_args[j].bucket_timing_info.unpack_bucket_resp_time;
            (*unpack_bucket_req_time) += thread_args[j].bucket_timing_info.unpack_bucket_req_time;
            (*calculate_knn_time) += thread_args[j].bucket_timing_info.calculate_knn_time;
            (*pack_bucket_resp_time) += thread_args[j].bucket_timing_info.pack_bucket_resp_time;

            for(unsigned int k = 0; k < thread_args[j].knn_answer.GetValueAtIndex(i).size(); k++)
            {
                knn.emplace_back(thread_args[j].knn_answer.GetValueAtIndex(i).at(k));
            }
        }
        knn_all_queries.emplace_back(knn);
        if (queries_size != 1) {
            knn.clear();
        }
    }
    knn_answer->DistanceCalculation(dataset,
            queries_multiple_points,
            knn_all_queries,
            number_of_nearest_neighbors,
            1);

    (*create_bucket_req_time) = (*create_bucket_req_time)/number_of_bucket_servers;
    (*unpack_bucket_resp_time) = (*unpack_bucket_resp_time)/number_of_bucket_servers;
    (*unpack_bucket_req_time) = (*unpack_bucket_req_time)/number_of_bucket_servers;
    (*calculate_knn_time) = (*calculate_knn_time)/number_of_bucket_servers;
    (*pack_bucket_resp_time) = (*pack_bucket_resp_time)/number_of_bucket_servers;
}

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const MultiplePoints &dataset,
        const MultiplePoints &queries_multiple_points,
        const unsigned int queries_size,
        const unsigned int query_dimensions,
        const unsigned int number_of_bucket_servers,
        const unsigned int number_of_nearest_neighbors,
        loadgen_index::ResponseIndexKnnQueries* index_reply)
{
    DistCalc knn_answer;
    uint64_t create_bucket_req_time = 0, unpack_bucket_resp_time = 0, unpack_bucket_req_time = 0, calculate_knn_time = 0, pack_bucket_resp_time = 0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t start_time = tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
    MergeFromResponseMap(response_data,
            dataset,
            queries_multiple_points,
            queries_size,
            query_dimensions,
            number_of_bucket_servers,
            number_of_nearest_neighbors,
            &knn_answer,
            &create_bucket_req_time,
            &unpack_bucket_resp_time,
            &unpack_bucket_req_time,
            &calculate_knn_time,
            &pack_bucket_resp_time);
    gettimeofday(&tv, NULL);
    uint64_t end_time = tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
    index_reply->set_merge_time((end_time-start_time));
    // Convert K-NN into form suitable for GRPC.
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
    PackIndexServiceResponseFromMap(knn_answer,
            response_data,
            number_of_bucket_servers,
            index_reply);
    gettimeofday(&tv, NULL);
    end_time = tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
    index_reply->set_pack_index_resp_time((end_time-start_time));
    index_reply->set_create_bucket_req_time(create_bucket_req_time);
    index_reply->set_unpack_bucket_resp_time(unpack_bucket_resp_time);
    index_reply->set_unpack_bucket_req_time(unpack_bucket_req_time);
    index_reply->set_calculate_knn_time(calculate_knn_time);
    index_reply->set_pack_bucket_resp_time(pack_bucket_resp_time);
    index_reply->set_number_of_bucket_servers(number_of_bucket_servers);
}

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
        uint64_t* pack_bucket_resp_time)
{
    std::vector<std::vector<uint32_t>> knn_all_queries;
    std::vector<uint32_t> knn;
    for(unsigned int i = 0; i < queries_size; i++)
    {
        for(unsigned int j = 0; j < number_of_bucket_servers; j++)
        {
            unsigned int neighbors_size = response_data[j].knn_answer->GetValueAtIndex(i).size();
            for(unsigned int k = 0; k < neighbors_size; k++)
            {
                knn.emplace_back(response_data[j].knn_answer->GetValueAtIndex(i).at(k));
            }
        }
        knn_all_queries.emplace_back(knn);
        if (queries_size != 1) {
            knn.clear();
        }
    }
    knn_answer->DistanceCalculation(dataset,
            queries_multiple_points,
            knn_all_queries,
            number_of_nearest_neighbors,
            1);
    /* Calculate the avergae time for all components from different
       bucket servers and send the mean as the final time.*/
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        (*create_bucket_req_time) += response_data[i].bucket_timing_info->create_bucket_request_time;
        (*unpack_bucket_resp_time) += response_data[i].bucket_timing_info->unpack_bucket_resp_time;
        (*unpack_bucket_req_time) += response_data[i].bucket_timing_info->unpack_bucket_req_time;
        (*calculate_knn_time) += response_data[i].bucket_timing_info->calculate_knn_time;
        (*pack_bucket_resp_time) += response_data[i].bucket_timing_info->pack_bucket_resp_time;
    }
    (*create_bucket_req_time) = (*create_bucket_req_time)/number_of_bucket_servers;
    (*unpack_bucket_resp_time) = (*unpack_bucket_resp_time)/number_of_bucket_servers;
    (*unpack_bucket_req_time) = (*unpack_bucket_req_time)/number_of_bucket_servers;
    (*calculate_knn_time) = (*calculate_knn_time)/number_of_bucket_servers;
    (*pack_bucket_resp_time) = (*pack_bucket_resp_time)/number_of_bucket_servers;
}

void PrintMatrix(const flann::Matrix<unsigned char> &matrix,
        const unsigned int rows,
        const unsigned int cols)
{
    for(unsigned int i = 0; i < rows; i++)
    {
        for(unsigned int j = 0; j < cols; j++)
        {
            std::cout << static_cast<unsigned>(matrix[i][j]) << " ";
        }
        std::cout << std::endl;
    }
}

/*void PrintPointIDs(std::vector<std::vector<uint32_t> >* point_ids_vec)
  {
  for(uint32_t i = 0; i < point_ids_vec->size(); i++)
  {
  std::cout << "Q" << i << " point IDs" << std::endl;
  for(uint32_t j = 0; j < point_ids_vec->at(i).size(); j++)
  {
  std::cout << static_cast<uint32_t> (point_ids_vec->at(i).at(j)) << " ";
  }
  std::cout << std::endl;
  }

  }*/

void InitializeTMs(const int num_tms,
        std::map<TMNames, TMConfig>* all_tms)
{
    for(int i = 0; i < num_tms; i++)
    {
        switch(i)
        {
            case 0: 
                {
                    TMNames tm_name = sip1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 0, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 1:
                {
                    TMNames tm_name = sdp1_20;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 20, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif

                    break;
                }
            case 2:
                {
                    TMNames tm_name = sdb1_50;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 50, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
#if 0
            case 3:
                {
                    TMNames tm_name = sdb30_50;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(30, 50, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 4:
                {
                    TMNames tm_name = sdb30_10;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(30, 10, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 5:
                {
                    TMNames tm_name = sdb40_30;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(40, 30, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 6:
                {
                    TMNames tm_name = sdb50_20;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(50, 20, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 7:
                {
                    TMNames tm_name = sdp1_50;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 50, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
#endif
        }

    }
}

void InitializeAsyncTMs(const int num_tms,
        std::map<AsyncTMNames, TMConfig>* all_tms)
{
    for(int i = 0; i < num_tms; i++)
    {
        switch(i)
        {
            case 0:
                {
                    AsyncTMNames tm_name = aip1_0_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<AsyncTMNames, TMConfig>(tm_name, TMConfig(1, 0, 1)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 1:
                {
                    AsyncTMNames tm_name = adp1_4_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<AsyncTMNames, TMConfig>(tm_name, TMConfig(1, 4, 1)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 2:
                {
                    AsyncTMNames tm_name = adb1_4_4;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<AsyncTMNames, TMConfig>(tm_name, TMConfig(1, 4, 4)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
        }

    }
}


void InitializeFMSyncTMs(const int num_tms,
        std::map<FMSyncTMNames, TMConfig>* all_tms)
{
    for(int i = 0; i < num_tms; i++)
    {
        switch(i)
        {
            case 0:
                {
                    FMSyncTMNames tm_name = sdb1_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(1, 1, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 1:
                {
                    FMSyncTMNames tm_name = sdb5_20;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(5, 20, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 2:
                {
                    FMSyncTMNames tm_name = sdb20_20;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(20, 20, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 3: 
                {                                                                                                                                FMSyncTMNames tm_name = last;
                    std::cout << "before assigning\n";
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(1, 50, 0)));
                    break;
                }
        }

    }
}

void InitializeFMAsyncTMs(const int num_tms,
        std::map<FMAsyncTMNames, TMConfig>* all_tms)
{
    for(int i = 0; i < num_tms; i++)
    {
        switch(i)
        {
            case 0:
                {
                    FMAsyncTMNames tm_name = adb1_1_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMAsyncTMNames, TMConfig>(tm_name, TMConfig(1, 1, 1)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 1:
                {
                    FMAsyncTMNames tm_name = adb4_4_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMAsyncTMNames, TMConfig>(tm_name, TMConfig(4, 4, 1)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 2:
                {
                    FMAsyncTMNames tm_name = adb4_1_4;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMAsyncTMNames, TMConfig>(tm_name, TMConfig(4, 1, 4)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 3:
                {   
                    FMAsyncTMNames tm_name = last_async;
                    std::cout << "before assigning\n";
                    all_tms->insert(std::pair<FMAsyncTMNames, TMConfig>(tm_name, TMConfig(1, 4, 4)));
                    break;
                }
        }

    }
}

