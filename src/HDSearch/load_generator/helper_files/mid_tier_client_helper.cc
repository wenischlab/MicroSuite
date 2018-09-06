#include <algorithm>
#include <iomanip>
#include "mid_tier_client_helper.h"
#include <openssl/md5.h> // To cache seen images using md5 checksum
#include <streambuf>

LoadGenCommandLineArgs* ParseLoadGenCommandLine(const int &argc,
        char** argv)
{
    struct LoadGenCommandLineArgs* load_gen_command_line_args = new struct LoadGenCommandLineArgs();
    if (argc == 10) {
        try
        {
            load_gen_command_line_args->queries_file_name = argv[1];
            load_gen_command_line_args->result_file_name = argv[2];
            load_gen_command_line_args->number_of_nearest_neighbors = std::stoul(argv[3], nullptr, 0); 
            load_gen_command_line_args->time_duration = std::stoul(argv[4], nullptr, 0);
            load_gen_command_line_args->qps = std::atof(argv[5]);
            load_gen_command_line_args->ip = argv[6];
            load_gen_command_line_args->timing_file_name = argv[7];
            load_gen_command_line_args->qps_file_name = argv[8];
            load_gen_command_line_args->util_file_name = argv[9];
        }
        catch(...)
        {
            CHECK(false, "Enter a valid number for number_of_nearest_neighbors/ valid string for queries path/ time to run the program/ QPS/ IP to bind to/ timing file name / qps file name/ util file name\n");
        }
    } else {
        CHECK(false, "Format: ./<loadgen_index_client> <queries file path> <K-NN result file path> <number_of_nearest_neighbors> <Time to run the program> <QPS> <IP to bind to> <timing file name> <QPS file name> <Util file name>\n");
    }
    return load_gen_command_line_args;
}

void ComputeMD5(const std::string &queries_file_name, std::string* checksum)
{
    unsigned char checksum_char[MD5_DIGEST_LENGTH];
    FILE* queries_file = fopen(queries_file_name.c_str(), "rb");
    MD5_CTX mdContext;
    int bytes = 0;
    unsigned char data[1024];
    CHECK((queries_file), "ERROR: JPEG Query file cannot be opened\n");
    MD5_Init (&mdContext);
    while ((bytes = fread(data, 1, 1024, queries_file)) != 0)
    {
        MD5_Update(&mdContext, data, bytes);
    }
    MD5_Final(checksum_char, &mdContext);
    std::stringstream checksum_stream;
    checksum_stream.fill('0');

    for(int i = 0; i < MD5_DIGEST_LENGTH; i++) 
    {
        checksum_stream << std::setw(2) << std::hex <<(unsigned short)checksum_char[i];
    }
    (*checksum) = checksum_stream.str();
    fclose(queries_file);
}

bool FilePresent(const std::string &checksum)
{
    //std::string checksum_str(checksum);
    std::string cmd = "redis-cli get " + checksum;
    std::string result;
    try {
        result = ExecuteShellCommand(cmd.c_str());
    } catch( ... ) {
        CHECK(false, "ERROR: Checking if redis contains the checksum, failed\n");
    }
    if(result.size() == 1)
    {
        return false;
    }
    return true;
}

void GetFeatureVector(const std::string &checksum, Point* query)
{
    /* Call redis and get the feature vector as a string.
       Convert this string into a float vector and use this float
       vector to build MultiplePoints.*/
    std::string cmd = "redis-cli get " + checksum;
    std::string queries_str = ExecuteShellCommand(cmd.c_str());
    std::vector<std::string> queries_str_vec;
    StringSplit(queries_str, ' ', &queries_str_vec);
    MultiplePoints queries;
    queries.CreatePoint(queries_str_vec, query);
    CHECK((query->GetSize() == 2048), "ERROR: query dimensions must be 2048.\n");
}

void CallFeatureExtractor(const std::string &queries_file_name, 
        const std::string &checksum,
        Point* query)
{
    std::string cmd = "python /home/liush/highdimensionalsearch/dataset/ILSVRC2012_jpeg/classify_image_mod.py -m 2 -f " + queries_file_name;
    std::string queries_str = ExecuteShellCommand(cmd.c_str());
    std::vector<std::string> queries_str_vec;
    StringSplit(queries_str, ' ', &queries_str_vec);
    MultiplePoints queries;
    queries.CreatePoint(queries_str_vec, query);
    CHECK((query->GetSize() == 2048), "ERROR: query dimensions must be 2048.\n");
    /* Now that we have the feature vector,
       we must cache this feature vector in redis, as well as create
       a multiple points structure of queries.*/
    cmd = "redis-cli set " + checksum + " " + "\"" + queries_str + "\"";
    ExecuteShellCommand(cmd.c_str());
}

void CreateIndexServiceRequest(const MultiplePoints &queries,
        const uint64_t query_id,
        const unsigned &queries_size,
        const unsigned &number_of_nearest_neighbors,
        const int &dimension,
        const bool util_request,
        loadgen_index::LoadGenRequest* load_gen_request)
{
    // Add each query to the query list.
    for(unsigned int i = 0; i < queries_size; i++)
    {
        load_gen_request->add_query_id(query_id);
    }

    // Add number of nearest neighbors to the request.
    load_gen_request->set_number_nearest_neighbors(number_of_nearest_neighbors);
    load_gen_request->mutable_util_request()->set_util_request(util_request);
}

void UnpackIndexServiceResponse(const loadgen_index::ResponseIndexKnnQueries &index_reply, 
        DistCalc* knn_answer, 
        TimingInfo* timing_info,
        UtilInfo* util,
        PercentUtilInfo* percent_util)
{
    int queries_size = index_reply.neighbor_ids_size();
    uint32_t id_value = 0;
    int number_of_nearest_neighbors = index_reply.neighbor_ids(0).point_id_size();
    PointIDs point_ids_per_query(number_of_nearest_neighbors, 0);
    knn_answer->Initialize(queries_size, point_ids_per_query);
    int neighbor_ids_size = 0;
    for(int i = 0; i < queries_size; i++)
    {
        neighbor_ids_size = index_reply.neighbor_ids(i).point_id_size();
        for(int j = 0; j < neighbor_ids_size; j++)
        {
            id_value = index_reply.neighbor_ids(i).point_id(j);
            point_ids_per_query[j] = id_value;
        }
        knn_answer->AddValueToIndex(i, point_ids_per_query);
    }
    UnpackTimingInfo(index_reply, timing_info);
    UnpackUtilInfo(index_reply, util, percent_util);
}

void UnpackTimingInfo(const loadgen_index::ResponseIndexKnnQueries &index_reply,
        TimingInfo* timing_info)
{
    timing_info->unpack_loadgen_req_time = index_reply.unpack_loadgen_req_time();
    timing_info->get_point_ids_time = index_reply.get_point_ids_time();
    timing_info->create_bucket_req_time = index_reply.create_bucket_req_time();
    timing_info->unpack_bucket_resp_time = index_reply.unpack_bucket_resp_time();
    timing_info->merge_time = index_reply.merge_time();
    timing_info->pack_index_resp_time = index_reply.pack_index_resp_time();
    timing_info->unpack_bucket_req_time = index_reply.unpack_bucket_req_time();
    timing_info->calculate_knn_time = index_reply.calculate_knn_time();
    timing_info->pack_bucket_resp_time = index_reply.pack_bucket_resp_time();
    timing_info->index_time = index_reply.index_time();
}

void UnpackUtilInfo(const loadgen_index::ResponseIndexKnnQueries &index_reply,
        UtilInfo* util,
        PercentUtilInfo* percent_util_info)
{
    if (index_reply.util_response().util_present() == false) {
        util->util_info_present = false;
        return;
    }
    util->util_info_present = true;
    /*If util exists, we need to get the bucket and index utils.
      In order to do so, we must first
      get the deltas between the previous and current
      times. Getting index first.*/
    uint64_t index_user_time_delta = index_reply.util_response().index_util().user_time() - util->index_util.user_time;
    uint64_t index_system_time_delta = index_reply.util_response().index_util().system_time() - util->index_util.system_time;
    uint64_t index_io_time_delta = index_reply.util_response().index_util().io_time() - util->index_util.io_time;
    uint64_t index_idle_time_delta = index_reply.util_response().index_util().idle_time() - util->index_util.idle_time;

    /* We then get the total, to compute util as a fraction
       of the total time.*/
    uint64_t total_index_time_delta = index_user_time_delta + index_system_time_delta + index_io_time_delta + index_idle_time_delta;
    percent_util_info->index_util_percent.user_util = 100.0 * ((float)(index_user_time_delta)/(float)(total_index_time_delta));
    percent_util_info->index_util_percent.system_util = 100.0 * ((float)(index_system_time_delta)/(float)(total_index_time_delta));
    percent_util_info->index_util_percent.io_util = 100.0 * ((float)(index_io_time_delta)/(float)(total_index_time_delta));
    percent_util_info->index_util_percent.idle_util = 100.0 * ((float)(index_idle_time_delta)/(float)(total_index_time_delta));


    //std::cout << "index user = " << percent_util_info->index_util_percent.user_util << "index system = " << percent_util_info->index_util_percent.system_util << "index io = " << percent_util_info->index_util_percent.io_util << "index idle = " << percent_util_info->index_util_percent.idle_util << std::endl;

    /*Update the previous value as the value obtained from the
      response, so that we are ready for the next round.*/
    util->index_util.user_time = index_reply.util_response().index_util().user_time();
    util->index_util.system_time = index_reply.util_response().index_util().system_time();
    util->index_util.io_time = index_reply.util_response().index_util().io_time();
    util->index_util.idle_time = index_reply.util_response().index_util().idle_time();
    /* Buckets utils are then obtained for every
       bucket server.*/
    for(int i = 0; i < index_reply.util_response().bucket_util_size(); i++)
    {
        uint64_t bucket_user_time_delta = index_reply.util_response().bucket_util(i).user_time() - util->bucket_util[i].user_time;
        uint64_t bucket_system_time_delta = index_reply.util_response().bucket_util(i).system_time() - util->bucket_util[i].system_time;
        uint64_t bucket_io_time_delta = index_reply.util_response().bucket_util(i).io_time() - util->bucket_util[i].io_time;
        uint64_t bucket_idle_time_delta = index_reply.util_response().bucket_util(i).idle_time() - util->bucket_util[i].idle_time;
        uint64_t total_bucket_time_delta = bucket_user_time_delta + bucket_system_time_delta + bucket_io_time_delta + bucket_idle_time_delta;
        percent_util_info->bucket_util_percent[i].user_util = 100.0 * ((float)(bucket_user_time_delta)/(float)(total_bucket_time_delta));
        percent_util_info->bucket_util_percent[i].system_util = 100.0 * ((float)(bucket_system_time_delta)/(float)(total_bucket_time_delta));
        percent_util_info->bucket_util_percent[i].io_util = 100.0 * ((float)(bucket_io_time_delta)/(float)(total_bucket_time_delta));
        percent_util_info->bucket_util_percent[i].idle_util = 100.0 * ((float)(bucket_idle_time_delta)/(float)(total_bucket_time_delta));

        //std::cout << "bucket user = " << percent_util_info->bucket_util_percent[i].user_util << "bucket system = " << percent_util_info->bucket_util_percent[i].system_util << "bucket io = " << percent_util_info->bucket_util_percent[i].io_util << "bucket idle = " << percent_util_info->bucket_util_percent[i].idle_util << std::endl;
        util->bucket_util[i].user_time = index_reply.util_response().bucket_util(i).user_time();
        util->bucket_util[i].system_time = index_reply.util_response().bucket_util(i).system_time();
        util->bucket_util[i].io_time = index_reply.util_response().bucket_util(i).io_time();
        util->bucket_util[i].idle_time = index_reply.util_response().bucket_util(i).idle_time();
    }
}

void PrintKNNForAllQueries(const DistCalc &knn_answer)
{
    for(unsigned int i = 0; i < knn_answer.GetSize(); i++)
    {
        //std::cout << "Q" << i << ": " << std::endl;
        for(uint32_t j = 0; j < knn_answer.GetValueAtIndex(i).size(); j++)
        {
            std::cout << "r " << static_cast<uint32_t> (knn_answer.GetValueAtIndex(i).at(j))<< std::endl;
        }
        //std::cout << std::endl;
    }

}

void CreatePointsFromFile(const std::string &file_name, 
        MultiplePoints* multiple_points)
{
    multiple_points->CreateMultiplePoints(file_name);
}

void CreatePointsFromBinFile(const std::string &file_name,
        MultiplePoints* multiple_points)
{
    multiple_points->CreateMultiplePointsFromBinFile(file_name);
}

void WriteKNNToFile(const std::string &knn_file_name, const DistCalc &knn_answer)
{
    std::ofstream knn_file(knn_file_name);
    // Error out if file could not be opened.
    CHECK(knn_file.good(), "ERROR: Bucket client could not open file to write KNN answer.\n");
    for(unsigned int i = 0 ; i < knn_answer.GetSize(); i++)
    {
        // Insert blank line to indicate knn for next query.
        //knn_file << "\n";
        CHECK((knn_answer.GetValueAtIndex(i).size() > 0), "ERROR: At least one NN must be returned\n");
        for(unsigned int j = knn_answer.GetValueAtIndex(i).size() - 1; j >= 0; j--)
        {
            knn_file << knn_answer.GetValueAtIndex(i).at(j) + 1<< " ";
        }
        knn_file << "\n";
    }
    knn_file.close();
}

void CreateDatasetFromBinaryFile(const std::string &file_name, MultiplePoints* dataset)
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
    int size = stream_to_get_size.tellg();
    int dataset_size = (size/sizeof(float))/dataset_dimensions;
    CHECK((dataset_size >= 0), "ERROR: Negative number of points in the dataset\n");
    /* Initialize dataset to contain 0s, 
       this is so that we can directly index every point/value after this.*/
    Point p(dataset_dimensions, 0.0);
    dataset->Resize(dataset_size, p);

    //Read each point (dimensions = 2048) and create a set of multiple points.
    float values[dataset_dimensions];
    for(int m = 0; m < dataset_size; m++)
    {
        if(fread(values, sizeof(float), dataset_dimensions, dataset_binary) == dataset_dimensions)
        {
            p.CreatePointFromFloatArray(values, dataset_dimensions);
            dataset->SetPoint(m, p);

        } else {
            break;
        }
    }
}


void PrintStatistics(const TimingInfo &timing_info)
{
    //std::cout << "\n***********************************************************************" << std::endl;
    //std::cout << "TIMING STATISTICS\n";
    std::cout << std::endl;
    std::cout << "LOADGEN: Time to create queries from file = " << (timing_info.create_queries_time/1000) << std::endl;
    std::cout << "LOADGEN: Time to create a request to the index server = " << (timing_info.create_index_req_time/1000) << std::endl;
    std::cout << "INDEX: Time to unpack load generator's request = " << (timing_info.unpack_loadgen_req_time/1000) << std::endl;
    std::cout << "INDEX: Time to get point IDs by LSH = " << (timing_info.get_point_ids_time/1000) << std::endl;
    std::cout << "INDEX: Time to create a request to bucket server = " << (timing_info.create_bucket_req_time/1000) << std::endl;
    std::cout << "BUCKET: Time to unpack request to bucket server = " << (timing_info.unpack_bucket_req_time/1000) << std::endl;
    std::cout << "BUCKET: Time to calculate K-NN (Distance computations) = " << (timing_info.calculate_knn_time/1000) << std::endl;
    std::cout << "BUCKET: Time to pack bucket server response = " << (timing_info.pack_bucket_resp_time/1000) << std::endl;
    std::cout << "INDEX: Time to unpack bucket service response = " << (timing_info.unpack_bucket_resp_time/1000) << std::endl;
    std::cout << "INDEX: Time to merge responses from all bucket servers = " << (timing_info.merge_time/1000) << std::endl;
    std::cout << "INDEX: Time to pack index service response = " << (timing_info.pack_index_resp_time/1000) << std::endl;
    std::cout << "LOADGEN: Time to unpack response from the index server = " << (timing_info.unpack_index_resp_time/1000) << std::endl;
    //std::cout << "***********************************************************************" << std::endl;
}

void WriteStatisticsToFile(std::ofstream &statistics_file_name, 
        const TimingInfo &timing_info)
{
    std::ofstream statistics_file;
    statistics_file << "LOADGEN: Time to create queries from file = " << timing_info.create_queries_time << " micro seconds" << std::endl;
    statistics_file << "LOADGEN: Time to create a request to the index server = " << timing_info.create_index_req_time << " micro seconds" << std::endl;
    statistics_file << "INDEX: Time to unpack load generator's request = " << timing_info.unpack_loadgen_req_time << " micro seconds" << std::endl;
    statistics_file << "INDEX: Time to get point IDs by LSH = " << timing_info.get_point_ids_time << " micro seconds" << std::endl;
    statistics_file << "INDEX: Time to create a request to bucket server = " << timing_info.create_bucket_req_time << " micro seconds" << std::endl;
    statistics_file << "BUCKET: Time to unpack request to bucket server = " << timing_info.unpack_bucket_req_time << " micro seconds" << std::endl;
    statistics_file << "BUCKET: Time to calculate K-NN (Distance computations) = " << timing_info.calculate_knn_time << " micro seconds" << std::endl;
    statistics_file << "BUCKET: Time to pack bucket server response = " << timing_info.pack_bucket_resp_time << " micro seconds" << std::endl;
    statistics_file << "INDEX: Time to unpack bucket service response = " << timing_info.unpack_bucket_resp_time << " micro seconds" << std::endl;
    statistics_file << "INDEX: Time to merge responses from all bucket servers = " << timing_info.merge_time << " micro seconds" << std::endl;
    statistics_file << "INDEX: Time to pack index service response = " << timing_info.pack_index_resp_time << " micro seconds" << std::endl;
    statistics_file << "LOADGEN: Time to unpack response from the index server = " << timing_info.unpack_index_resp_time << " micro seconds" << std::endl;
}

void WriteTimingInfoToFile(std::ofstream &timing_file, 
        const TimingInfo &timing_info)
{
    timing_file << timing_info.create_index_req_time << " ";
    timing_file << timing_info.update_index_util_time << " ";
    timing_file << timing_info.unpack_loadgen_req_time << " ";
    timing_file << timing_info.get_point_ids_time << " ";
    timing_file << timing_info.get_bucket_responses_time << " ";
    timing_file << timing_info.create_bucket_req_time << " ";
    timing_file << timing_info.unpack_bucket_req_time << " ";
    timing_file << timing_info.calculate_knn_time << " ";
    timing_file << timing_info.pack_bucket_resp_time << " ";
    timing_file << timing_info.unpack_bucket_resp_time << " ";
    timing_file << timing_info.merge_time << " ";
    timing_file << timing_info.pack_index_resp_time << " ";
    timing_file << timing_info.unpack_index_resp_time << " ";
    timing_file << timing_info.total_resp_time << "\n";
    timing_file.close();
}

void UpdateGlobalTimingStats(const TimingInfo &timing_info,
        GlobalStats* global_stats)
{
    /*global_stats->timing_info.create_index_req_time += timing_info.create_index_req_time;
      global_stats->timing_info.unpack_loadgen_req_time += timing_info.unpack_loadgen_req_time;
      global_stats->timing_info.get_point_ids_time += timing_info.get_point_ids_time;
      global_stats->timing_info.create_bucket_req_time += timing_info.create_bucket_req_time;
      global_stats->timing_info.unpack_bucket_req_time += timing_info.unpack_bucket_req_time;
      global_stats->timing_info.calculate_knn_time += timing_info.calculate_knn_time;
      global_stats->timing_info.pack_bucket_resp_time += timing_info.pack_bucket_resp_time;
      global_stats->timing_info.unpack_bucket_resp_time += timing_info.unpack_bucket_resp_time;
      global_stats->timing_info.merge_time += timing_info.merge_time;
      global_stats->timing_info.pack_index_resp_time += timing_info.pack_index_resp_time;
      global_stats->timing_info.unpack_index_resp_time += timing_info.unpack_index_resp_time;
      global_stats->timing_info.update_index_util_time += timing_info.update_index_util_time;
      global_stats->timing_info.get_bucket_responses_time += timing_info.get_bucket_responses_time;
      global_stats->timing_info.total_resp_time += timing_info.total_resp_time;*/
    global_stats->timing_info.push_back(timing_info);
}

void UpdateGlobalUtilStats(PercentUtilInfo* percent_util_info,
        const unsigned int number_of_bucket_servers,
        GlobalStats* global_stats)
{
    global_stats->percent_util_info.index_util_percent.user_util += percent_util_info->index_util_percent.user_util;
    global_stats->percent_util_info.index_util_percent.system_util += percent_util_info->index_util_percent.system_util;
    global_stats->percent_util_info.index_util_percent.io_util += percent_util_info->index_util_percent.io_util;
    global_stats->percent_util_info.index_util_percent.idle_util += percent_util_info->index_util_percent.idle_util;
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        global_stats->percent_util_info.bucket_util_percent[i].user_util += percent_util_info->bucket_util_percent[i].user_util;
        global_stats->percent_util_info.bucket_util_percent[i].system_util += percent_util_info->bucket_util_percent[i].system_util;
        global_stats->percent_util_info.bucket_util_percent[i].io_util += percent_util_info->bucket_util_percent[i].io_util;
        global_stats->percent_util_info.bucket_util_percent[i].idle_util += percent_util_info->bucket_util_percent[i].idle_util;
    }
}

void PrintTime(std::vector<uint64_t> time_vec)
{
    uint64_t size = time_vec.size();
    std::cout << (float)time_vec[0.1*size]/1000.0 << " " << (float)time_vec[0.2*size]/1000.0 << " " << (float)time_vec[0.3*size]/1000.0 << " " << (float)time_vec[0.4*size]/1000.0 << " " << (float)time_vec[0.5*size]/1000.0 << " " << (float)time_vec[0.6*size]/1000.0 << " " << (float)time_vec[0.7*size]/1000.0 << " " << (float)time_vec[0.8*size]/1000.0 << " " << (float)time_vec[0.9*size]/1000.0 << " " << (float)(float)time_vec[0.95*size]/1000.0 << " " << (float)(float)time_vec[0.99*size]/1000.0 << " " << (float)(float)time_vec[0.999*size]/1000.0 << " ";
}

float ComputeQueryCost(const GlobalStats &global_stats,
        const unsigned int util_requests,
        const unsigned int number_of_bucket_servers,
        float achieved_qps)
{
    // First add up all the index utils - this is % data
    unsigned int total_utilization = (global_stats.percent_util_info.index_util_percent.user_util/util_requests) + (global_stats.percent_util_info.index_util_percent.system_util/util_requests) + (global_stats.percent_util_info.index_util_percent.io_util/util_requests);
#if 0
    // Now add all bucket utils to it - again % data
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        total_utilization +=  (global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests) + (global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests) + (global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests);
    }
#endif
    // Now that we have total utilization, we see how many cpus that's worth.
    float cpus = (float)(total_utilization)/100.0;
    float cost = (float)(cpus)/(float)(achieved_qps);
    return cost;
}

void PrintGlobalStats(const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd)
{
    /*std::cout << global_stats.timing_info.create_index_req_time/responses_recvd << "," << global_stats.timing_info.update_index_util_time/responses_recvd << "," << global_stats.timing_info.unpack_loadgen_req_time/responses_recvd << "," << global_stats.timing_info.get_point_ids_time/responses_recvd << "," << global_stats.timing_info.get_bucket_responses_time/responses_recvd << "," << global_stats.timing_info.create_bucket_req_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_req_time/responses_recvd << "," << global_stats.timing_info.calculate_knn_time/responses_recvd << "," << global_stats.timing_info.pack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.merge_time/responses_recvd << "," << global_stats.timing_info.pack_index_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_index_resp_time/responses_recvd << "," << global_stats.timing_info.total_resp_time/responses_recvd << "," << global_stats.percent_util_info.index_util_percent.user_util/util_requests << "," << global_stats.percent_util_info.index_util_percent.system_util/util_requests << "," << global_stats.percent_util_info.index_util_percent.io_util/util_requests << "," << global_stats.percent_util_info.index_util_percent.idle_util/util_requests << ",";*/

    std::vector<uint64_t> total_response_time, create_index_req, update_index_util, unpack_loadgen_req, get_point_ids, get_bucket_responses, create_bucket_req, unpack_bucket_req, calculate_knn, pack_bucket_resp, unpack_bucket_resp, merge, pack_index_resp, unpack_index_resp, index_time;
    unsigned int timing_info_size = global_stats.timing_info.size();
    for(unsigned int i = 0; i < timing_info_size; i++)
    {
        total_response_time.push_back(global_stats.timing_info[i].total_resp_time);
        create_index_req.push_back(global_stats.timing_info[i].create_index_req_time);
        update_index_util.push_back(global_stats.timing_info[i].update_index_util_time);
        unpack_loadgen_req.push_back(global_stats.timing_info[i].unpack_loadgen_req_time);
        get_point_ids.push_back(global_stats.timing_info[i].get_point_ids_time);
        get_bucket_responses.push_back(global_stats.timing_info[i].get_bucket_responses_time);
        create_bucket_req.push_back(global_stats.timing_info[i].create_bucket_req_time);
        unpack_bucket_req.push_back(global_stats.timing_info[i].unpack_bucket_req_time);
        calculate_knn.push_back(global_stats.timing_info[i].calculate_knn_time);
        pack_bucket_resp.push_back(global_stats.timing_info[i].pack_bucket_resp_time);
        unpack_bucket_resp.push_back(global_stats.timing_info[i].unpack_bucket_resp_time);
        merge.push_back(global_stats.timing_info[i].merge_time);
        pack_index_resp.push_back(global_stats.timing_info[i].pack_index_resp_time);
        unpack_index_resp.push_back(global_stats.timing_info[i].unpack_index_resp_time);
        index_time.push_back(global_stats.timing_info[i].index_time);
    }
    std::sort(total_response_time.begin(), total_response_time.end());
    std::sort(create_index_req.begin(), create_index_req.end());
    std::sort(update_index_util.begin(), update_index_util.end());
    std::sort(unpack_loadgen_req.begin(), unpack_loadgen_req.end());
    std::sort(get_point_ids.begin(), get_point_ids.end());
    std::sort(get_bucket_responses.begin(), get_bucket_responses.end());
    std::sort(create_bucket_req.begin(), create_bucket_req.end());
    std::sort(unpack_bucket_req.begin(), unpack_bucket_req.end());
    std::sort(calculate_knn.begin(), calculate_knn.end());
    std::sort(pack_bucket_resp.begin(), pack_bucket_resp.end());
    std::sort(unpack_bucket_resp.begin(), unpack_bucket_resp.end());
    std::sort(merge.begin(), merge.end());
    std::sort(pack_index_resp.begin(), pack_index_resp.end());
    std::sort(unpack_index_resp.begin(), unpack_index_resp.end());
    std::sort(index_time.begin(), index_time.end());
    std::cout << "\n Total response time \n"; 
    PrintTime(total_response_time);
    std::cout << std::endl;
    std::cout << "\n Index creation time ";
    PrintTime(create_index_req);
    std::cout << "\n Update index util time ";
    PrintTime(update_index_util);
    std::cout << "\n Unpack loadgen request time ";
    PrintTime(unpack_loadgen_req);
    std::cout << "\n Get point ids time \n";
    PrintTime(get_point_ids);
    std::cout << "\n Total time taken by index server: \n";
    PrintTime(index_time);
    //std::cout << std::endl;
    std::cout << "\n Get bucket responses time \n";
    PrintTime(get_bucket_responses);
    std::cout << "\n Create bucket request time ";
    PrintTime(create_bucket_req);
    std::cout << "\n Unpack bucket request time ";
    PrintTime(unpack_bucket_req);
    std::cout << "\n Calculate knn time \n";
    PrintTime(calculate_knn);
    std::cout << "\n Pack bucket response time ";
    PrintTime(pack_bucket_resp);
    std::cout << "\n Unpack bucket response time ";
    PrintTime(unpack_bucket_resp);
    std::cout << "\n Merge time ";
    PrintTime(merge);
    std::cout << "\n Pack index response time ";
    PrintTime(pack_index_resp);
    std::cout << "\n Unpack index response time ";
    PrintTime(unpack_index_resp);
    /*for(int i = 0; i < number_of_bucket_servers; i++)
      {
      std::cout << global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].idle_util/util_requests << ",";
      }*/
    std::cout << std::endl;
}

void PrintLatency(const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd)
{
    std::vector<uint64_t> total_response_time;
    unsigned int timing_info_size = global_stats.timing_info.size();
    for(unsigned int i = 0; i < timing_info_size; i++)
    {
        total_response_time.push_back(global_stats.timing_info[i].total_resp_time);
    }
    std::sort(total_response_time.begin(), total_response_time.end());
    //uint64_t size = total_response_time.size();
    //std::cout << (float)total_response_time[0.5*size]/1000.0 << " " << (float)total_response_time[0.99*size]/1000.0 << " ";
    PrintTime(total_response_time);
}

void PrintUtil(const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests)
{
    std::cout << (global_stats.percent_util_info.index_util_percent.user_util/util_requests) + (global_stats.percent_util_info.index_util_percent.system_util/util_requests) + (global_stats.percent_util_info.index_util_percent.io_util/util_requests) << " ";
    std::cout.flush();
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        std::cout << (global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests) + (global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests) + (global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests) << " ";
        std::cout.flush();
    }
}

void WriteToUtilFile(const std::string util_file_name,
        const GlobalStats &global_stats,
        const unsigned int number_of_bucket_servers,
        const unsigned int util_requests)
{
    std::ofstream util_file;
    util_file.open(util_file_name, std::ios_base::app);
    CHECK(util_file.good(), "ERROR: Util file could not be opened.\n");
    util_file << number_of_bucket_servers << " ";
    /*util_file << global_stats.percent_util_info.index_util_percent.user_util/util_requests << " ";
      util_file << global_stats.percent_util_info.index_util_percent.system_util/util_requests << " ";
      util_file << global_stats.percent_util_info.index_util_percent.io_util/util_requests << " ";*/
    util_file << (global_stats.percent_util_info.index_util_percent.user_util/util_requests) + (global_stats.percent_util_info.index_util_percent.system_util/util_requests) + (global_stats.percent_util_info.index_util_percent.io_util/util_requests) << " ";
    //util_file << global_stats.percent_util_info.index_util_percent.idle_util/util_requests << " ";
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        /*util_file << global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests << " ";
          util_file << global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests << " ";
          util_file << global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests << " ";*/
        util_file << (global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests) + (global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests) + (global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests) << " ";
        //util_file << global_stats.percent_util_info.bucket_util_percent[i].idle_util/util_requests << " ";
    }
    util_file << "\n";
}

void PrintTimingHistogram(std::vector<uint64_t> &time_vec)
{
    std::sort(time_vec.begin(), time_vec.end());
    uint64_t size = time_vec.size();

    std::cout << (float)time_vec[0.1 * size]/1000.0 << " " << (float)time_vec[0.2 * size]/1000.0 << " " << (float)time_vec[0.3 * size]/1000.0 << " " << (float)time_vec[0.4 * size]/1000.0 << " " << (float)time_vec[0.5 * size]/1000.0 << " " << (float)time_vec[0.6 * size]/1000.0 << " " << (float)time_vec[0.7 * size]/1000.0 << " " << (float)time_vec[0.8 * size]/1000.0 << " " << (float)time_vec[0.9 * size]/1000.0 << " " << (float)time_vec[0.99 * size]/1000.0 << " ";
}

void ResetMetaStats(GlobalStats* meta_stats,
        int number_of_bucket_servers)
{
    meta_stats->timing_info.clear();
    meta_stats->percent_util_info.index_util_percent.user_util = 0;
    meta_stats->percent_util_info.index_util_percent.system_util = 0;
    meta_stats->percent_util_info.index_util_percent.io_util = 0;
    meta_stats->percent_util_info.index_util_percent.idle_util = 0;
    for(unsigned int i = 0; i < number_of_bucket_servers; i++)
    {
        meta_stats->percent_util_info.bucket_util_percent[i].user_util = 0;
        meta_stats->percent_util_info.bucket_util_percent[i].system_util = 0;
        meta_stats->percent_util_info.bucket_util_percent[i].io_util = 0;
        meta_stats->percent_util_info.bucket_util_percent[i].idle_util = 0;
    }
}
