#include "loadgen_router_client_helper.h"

LoadGenCommandLineArgs* ParseLoadGenCommandLine(const int &argc,
        char** argv)
{
    struct LoadGenCommandLineArgs* load_gen_command_line_args = new struct LoadGenCommandLineArgs();
    if (argc == 8) {
        try
        {
            load_gen_command_line_args->queries_file_name = argv[1];
            load_gen_command_line_args->result_file_name = argv[2];
            load_gen_command_line_args->time_duration = std::stoul(argv[3], nullptr, 0);
            load_gen_command_line_args->qps = std::atof(argv[4]);
            load_gen_command_line_args->ip = argv[5];
            load_gen_command_line_args->get_ratio = atoi(argv[6]);
            load_gen_command_line_args->set_ratio = atoi(argv[7]);
        }
        catch(...)
        {
            CHECK(false, "Enter a valid number for valid string for queries path/ time to run the program/ QPS/ IP to bind to/ get ratio/ set ratio\n");
        }
    } else {
        CHECK(false, "Format: ./<loadgen_router_client> <queries file path> <result file path> <Time to run the program> <QPS> <IP to bind to> <get ratio> <set ratio>\n");
    }
    return load_gen_command_line_args;
}

void CreateQueriesFromFile(std::string queries_file_name,
        std::vector<std::pair<std::string, std::string> >* queries)
{
    std::ifstream file(queries_file_name);
    CHECK((file.good()), "Cannot create queries from file because file does not exist\n");
    std::string line;
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buf(line);
        std::istream_iterator<std::string> begin(buf), end;
        std::vector<std::string> tokens(begin, end);

        std::string key = "", value = "";
        int itr = 0;
        for(auto& s: tokens)
        {
            if (itr == 0) {
                key = s;
            } else {
                value = s;
            }
            queries->emplace_back(std::make_pair(key, value));
            itr++;
        }
    }
}

void CreateQueriesFromBinFile(std::string file_name,
        std::vector<std::pair<std::string, std::string> >* queries)
{
    int dataset_dimensions = 2;
    FILE *dataset_binary = fopen(file_name.c_str(), "r");

    if(!dataset_binary)
    {
        CHECK(false, "ERROR: Could not open queries file\n");
    }
    /* Get the size of the file, so that we can initialize number
       of points, given 2048 dimensions.*/
    std::ifstream stream_to_get_size(file_name, std::ios::binary | std::ios::in);
    stream_to_get_size.seekg(0, std::ios::end);
    int size = stream_to_get_size.tellg();
    int dataset_size = (size/sizeof(long))/dataset_dimensions;
    CHECK((dataset_size >= 0), "ERROR: Negative number of elements in the queries file\n");
    /* Initialize dataset to contain 0s, 
       this is so that we can directly index every point/value after this.*/
    queries->resize(dataset_size, std::pair<std::string, std::string>());

    //Read each point (dimensions = 2048) and create a set of multiple points.
    long values[dataset_dimensions];
    std::string key = "", value = "";
    for(int m = 0; m < dataset_size; m++)
    {
        if(fread(values, sizeof(long), dataset_dimensions, dataset_binary) == dataset_dimensions)
        {
            key = std::to_string(values[0]);
            value = std::to_string(values[1]);
            (*queries)[m] = std::make_pair(key, value);
        } else {
            break;
        }
    }
}

void CreateRouterServiceRequest(const std::string key,
        const std::string value,
        const int operation,
        const bool util_request,
        router::RouterRequest* router_request)
{
    router_request->set_key(key);
    router_request->set_value(value);
    router_request->set_operation(operation);
    router_request->mutable_util_request()->set_util_request(util_request);
}

void UnpackRouterServiceResponse(const router::LookupResponse &router_reply,
        std::string* value,
        TimingInfo* timing_info,                                                    
        UtilInfo* previous_util,                                                                                                                                                                                                                                                                                                                                                  PercentUtilInfo* percent_util_info)
{
    *value = router_reply.value();
    UnpackTimingInfo(router_reply, timing_info);
    UnpackUtilInfo(router_reply, previous_util, percent_util_info);
}

/* Following two functions are helpers to the above function:
   They unpack stats.*/
void UnpackTimingInfo(const router::LookupResponse &router_reply,
        TimingInfo* timing_info)
{
    timing_info->unpack_router_req_time = router_reply.unpack_router_req_time();
    timing_info->create_lookup_srv_req_time = router_reply.create_lookup_srv_req_time();
    timing_info->unpack_lookup_srv_resp_time = router_reply.unpack_lookup_srv_resp_time();
    timing_info->pack_router_resp_time = router_reply.pack_router_resp_time();
    timing_info->unpack_lookup_srv_req_time = router_reply.unpack_lookup_srv_req_time();
    timing_info->lookup_srv_time = router_reply.lookup_srv_time();
    timing_info->pack_lookup_srv_resp_time = router_reply.pack_lookup_srv_resp_time();
    timing_info->router_time = router_reply.router_time();
}

void UnpackUtilInfo(const router::LookupResponse &router_reply,
        UtilInfo* util,
        PercentUtilInfo* percent_util_info)
{
    if (router_reply.util_response().util_present() == false) {
        util->util_info_present = false;
        return;
    }
    util->util_info_present = true;
    /*If util exists, we need to get the lookup_srv and router utils.
      In order to do so, we must first
      get the deltas between the previous and current
      times. Getting router first.*/
    uint64_t router_user_time_delta = router_reply.util_response().router_util().user_time() - util->router_util.user_time;
    uint64_t router_system_time_delta = router_reply.util_response().router_util().system_time() - util->router_util.system_time;
    uint64_t router_io_time_delta = router_reply.util_response().router_util().io_time() - util->router_util.io_time;
    uint64_t router_idle_time_delta = router_reply.util_response().router_util().idle_time() - util->router_util.idle_time;

    /* We then get the total, to compute util as a fraction
       of the total time.*/
    uint64_t total_router_time_delta = router_user_time_delta + router_system_time_delta + router_io_time_delta + router_idle_time_delta;
    percent_util_info->router_util_percent.user_util = 100.0 * ((float)(router_user_time_delta)/(float)(total_router_time_delta));
    percent_util_info->router_util_percent.system_util = 100.0 * ((float)(router_system_time_delta)/(float)(total_router_time_delta));
    percent_util_info->router_util_percent.io_util = 100.0 * ((float)(router_io_time_delta)/(float)(total_router_time_delta));
    percent_util_info->router_util_percent.idle_util = 100.0 * ((float)(router_idle_time_delta)/(float)(total_router_time_delta));


    //std::cout << "router user = " << percent_util_info->router_util_percent.user_util << "router system = " << percent_util_info->router_util_percent.system_util << "router io = " << percent_util_info->router_util_percent.io_util << "router idle = " << percent_util_info->router_util_percent.idle_util << std::endl;

    /*Update the previous value as the value obtained from the
      response, so that we are ready for the next round.*/
    util->router_util.user_time = router_reply.util_response().router_util().user_time();
    util->router_util.system_time = router_reply.util_response().router_util().system_time();
    util->router_util.io_time = router_reply.util_response().router_util().io_time();
    util->router_util.idle_time = router_reply.util_response().router_util().idle_time();
    /* Buckets utils are then obtained for every
       lookup_srv server.*/
    for(int i = 0; i < router_reply.util_response().lookup_srv_util_size(); i++)
    {
        uint64_t lookup_srv_user_time_delta = router_reply.util_response().lookup_srv_util(i).user_time() - util->lookup_srv_util[i].user_time;
        uint64_t lookup_srv_system_time_delta = router_reply.util_response().lookup_srv_util(i).system_time() - util->lookup_srv_util[i].system_time;
        uint64_t lookup_srv_io_time_delta = router_reply.util_response().lookup_srv_util(i).io_time() - util->lookup_srv_util[i].io_time;
        uint64_t lookup_srv_idle_time_delta = router_reply.util_response().lookup_srv_util(i).idle_time() - util->lookup_srv_util[i].idle_time;
        uint64_t total_lookup_srv_time_delta = lookup_srv_user_time_delta + lookup_srv_system_time_delta + lookup_srv_io_time_delta + lookup_srv_idle_time_delta;
        percent_util_info->lookup_srv_util_percent[i].user_util = 100.0 * ((float)(lookup_srv_user_time_delta)/(float)(total_lookup_srv_time_delta));
        percent_util_info->lookup_srv_util_percent[i].system_util = 100.0 * ((float)(lookup_srv_system_time_delta)/(float)(total_lookup_srv_time_delta));
        percent_util_info->lookup_srv_util_percent[i].io_util = 100.0 * ((float)(lookup_srv_io_time_delta)/(float)(total_lookup_srv_time_delta));
        percent_util_info->lookup_srv_util_percent[i].idle_util = 100.0 * ((float)(lookup_srv_idle_time_delta)/(float)(total_lookup_srv_time_delta));

        //std::cout << "lookup_srv user = " << percent_util_info->lookup_srv_util_percent[i].user_util << "lookup_srv system = " << percent_util_info->lookup_srv_util_percent[i].system_util << "lookup_srv io = " << percent_util_info->lookup_srv_util_percent[i].io_util << "lookup_srv idle = " << percent_util_info->lookup_srv_util_percent[i].idle_util << std::endl;
        util->lookup_srv_util[i].user_time = router_reply.util_response().lookup_srv_util(i).user_time();
        util->lookup_srv_util[i].system_time = router_reply.util_response().lookup_srv_util(i).system_time();
        util->lookup_srv_util[i].io_time = router_reply.util_response().lookup_srv_util(i).io_time();
        util->lookup_srv_util[i].idle_time = router_reply.util_response().lookup_srv_util(i).idle_time();
    }
}

void PrintValueForAllQueries(const std::string value)
{
    std::cout << value << std::endl;

}

void UpdateGlobalTimingStats(const TimingInfo &timing_info,
        GlobalStats* global_stats)
{
    /*global_stats->timing_info.create_router_req_time += timing_info.create_router_req_time;
      global_stats->timing_info.unpack_loadgen_req_time += timing_info.unpack_loadgen_req_time;
      global_stats->timing_info.get_point_ids_time += timing_info.get_point_ids_time;
      global_stats->timing_info.create_bucket_req_time += timing_info.create_bucket_req_time;
      global_stats->timing_info.unpack_bucket_req_time += timing_info.unpack_bucket_req_time;
      global_stats->timing_info.calculate_knn_time += timing_info.calculate_knn_time;
      global_stats->timing_info.pack_bucket_resp_time += timing_info.pack_bucket_resp_time;
      global_stats->timing_info.unpack_bucket_resp_time += timing_info.unpack_bucket_resp_time;
      global_stats->timing_info.merge_time += timing_info.merge_time;
      global_stats->timing_info.pack_router_resp_time += timing_info.pack_router_resp_time;
      global_stats->timing_info.unpack_router_resp_time += timing_info.unpack_router_resp_time;
      global_stats->timing_info.update_router_util_time += timing_info.update_router_util_time;
      global_stats->timing_info.get_bucket_responses_time += timing_info.get_bucket_responses_time;
      global_stats->timing_info.total_resp_time += timing_info.total_resp_time;*/
    global_stats->timing_info.push_back(timing_info);
}

void UpdateGlobalUtilStats(PercentUtilInfo* percent_util_info,
        const unsigned int number_of_lookup_servers,
        GlobalStats* global_stats)
{
    global_stats->percent_util_info.router_util_percent.user_util += percent_util_info->router_util_percent.user_util;
    global_stats->percent_util_info.router_util_percent.system_util += percent_util_info->router_util_percent.system_util;
    global_stats->percent_util_info.router_util_percent.io_util += percent_util_info->router_util_percent.io_util;
    global_stats->percent_util_info.router_util_percent.idle_util += percent_util_info->router_util_percent.idle_util;
    for(unsigned int i = 0; i < number_of_lookup_servers; i++)
    {
        global_stats->percent_util_info.lookup_srv_util_percent[i].user_util += percent_util_info->lookup_srv_util_percent[i].user_util;
        global_stats->percent_util_info.lookup_srv_util_percent[i].system_util += percent_util_info->lookup_srv_util_percent[i].system_util;
        global_stats->percent_util_info.lookup_srv_util_percent[i].io_util += percent_util_info->lookup_srv_util_percent[i].io_util;
        global_stats->percent_util_info.lookup_srv_util_percent[i].idle_util += percent_util_info->lookup_srv_util_percent[i].idle_util;
    }
}

void PrintTime(std::vector<uint64_t> time_vec)
{
    uint64_t size = time_vec.size();
    std::cout << (float)time_vec[0.1*size]/1000.0 << " " << (float)time_vec[0.2*size]/1000.0 << " " << (float)time_vec[0.3*size]/1000.0 << " " << (float)time_vec[0.4*size]/1000.0 << " " << (float)time_vec[0.5*size]/1000.0 << " " << (float)time_vec[0.6*size]/1000.0 << " " << (float)time_vec[0.7*size]/1000.0 << " " << (float)time_vec[0.8*size]/1000.0 << " " << (float)time_vec[0.9*size]/1000.0 << " " << (float)(float)time_vec[0.95*size]/1000.0 << " " << (float)(float)time_vec[0.99*size]/1000.0 << " " << (float)(float)time_vec[0.999*size]/1000.0 << " ";
}

float ComputeQueryCost(const GlobalStats &global_stats,
        const unsigned int util_requests,
        const unsigned int number_of_lookup_servers,
        float achieved_qps)
{
    // First add up all the router utils - this is % data
    unsigned int total_utilization = (global_stats.percent_util_info.router_util_percent.user_util/util_requests) + (global_stats.percent_util_info.router_util_percent.system_util/util_requests) + (global_stats.percent_util_info.router_util_percent.io_util/util_requests);
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
        const unsigned int number_of_lookup_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd)
{
    /*std::cout << global_stats.timing_info.create_router_req_time/responses_recvd << "," << global_stats.timing_info.update_router_util_time/responses_recvd << "," << global_stats.timing_info.unpack_loadgen_req_time/responses_recvd << "," << global_stats.timing_info.get_point_ids_time/responses_recvd << "," << global_stats.timing_info.get_bucket_responses_time/responses_recvd << "," << global_stats.timing_info.create_bucket_req_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_req_time/responses_recvd << "," << global_stats.timing_info.calculate_knn_time/responses_recvd << "," << global_stats.timing_info.pack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.merge_time/responses_recvd << "," << global_stats.timing_info.pack_router_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_router_resp_time/responses_recvd << "," << global_stats.timing_info.total_resp_time/responses_recvd << "," << global_stats.percent_util_info.router_util_percent.user_util/util_requests << "," << global_stats.percent_util_info.router_util_percent.system_util/util_requests << "," << global_stats.percent_util_info.router_util_percent.io_util/util_requests << "," << global_stats.percent_util_info.router_util_percent.idle_util/util_requests << ",";*/

    std::vector<uint64_t> total_response_time, create_router_req, update_router_util, unpack_router_req, get_lookup_srv_responses, create_lookup_srv_req, unpack_lookup_srv_req, lookup_srv_time, pack_lookup_srv_resp, unpack_lookup_srv_resp, merge, pack_router_resp, unpack_router_resp, router_time;
    unsigned int timing_info_size = global_stats.timing_info.size();
    for(unsigned int i = 0; i < timing_info_size; i++)
    {
        total_response_time.push_back(global_stats.timing_info[i].total_resp_time);
        create_router_req.push_back(global_stats.timing_info[i].create_router_req_time);
        update_router_util.push_back(global_stats.timing_info[i].update_router_util_time);
        unpack_router_req.push_back(global_stats.timing_info[i].unpack_router_req_time);
        get_lookup_srv_responses.push_back(global_stats.timing_info[i].get_lookup_srv_responses_time);
        create_lookup_srv_req.push_back(global_stats.timing_info[i].create_lookup_srv_req_time);
        unpack_lookup_srv_req.push_back(global_stats.timing_info[i].unpack_lookup_srv_req_time);
        lookup_srv_time.push_back(global_stats.timing_info[i].lookup_srv_time);
        pack_lookup_srv_resp.push_back(global_stats.timing_info[i].pack_lookup_srv_resp_time);
        unpack_lookup_srv_resp.push_back(global_stats.timing_info[i].unpack_lookup_srv_resp_time);
        pack_router_resp.push_back(global_stats.timing_info[i].pack_router_resp_time);
        unpack_router_resp.push_back(global_stats.timing_info[i].unpack_router_resp_time);
        router_time.push_back(global_stats.timing_info[i].router_time);
    }
    std::sort(total_response_time.begin(), total_response_time.end());
    std::sort(create_router_req.begin(), create_router_req.end());
    std::sort(update_router_util.begin(), update_router_util.end());
    std::sort(unpack_router_req.begin(), unpack_router_req.end());
    std::sort(get_lookup_srv_responses.begin(), get_lookup_srv_responses.end());
    std::sort(create_lookup_srv_req.begin(), create_lookup_srv_req.end());
    std::sort(unpack_lookup_srv_req.begin(), unpack_lookup_srv_req.end());
    std::sort(lookup_srv_time.begin(), lookup_srv_time.end());
    std::sort(pack_lookup_srv_resp.begin(), pack_lookup_srv_resp.end());
    std::sort(unpack_lookup_srv_resp.begin(), unpack_lookup_srv_resp.end());
    std::sort(pack_router_resp.begin(), pack_router_resp.end());
    std::sort(unpack_router_resp.begin(), unpack_router_resp.end());
    std::sort(router_time.begin(), router_time.end());
    std::cout << "\n Total response time \n"; 
    PrintTime(total_response_time);
    std::cout << std::endl;
    std::cout << "\n Index creation time ";
    PrintTime(create_router_req);
    std::cout << "\n Update router util time ";
    PrintTime(update_router_util);
    std::cout << "\n Unpack loadgen request time ";
    PrintTime(unpack_router_req);
    std::cout << "\n Total time taken by router server: \n";
    PrintTime(router_time);
    //std::cout << std::endl;
    std::cout << "\n Get bucket responses time \n";
    PrintTime(get_lookup_srv_responses);
    std::cout << "\n Create bucket request time ";
    PrintTime(create_lookup_srv_req);
    std::cout << "\n Unpack bucket request time ";
    PrintTime(unpack_lookup_srv_req);
    std::cout << "\n Calculate knn time \n";
    PrintTime(lookup_srv_time);
    std::cout << "\n Pack bucket response time ";
    PrintTime(pack_lookup_srv_resp);
    std::cout << "\n Unpack bucket response time ";
    PrintTime(unpack_lookup_srv_resp);
    std::cout << "\n Pack router response time ";
    PrintTime(pack_router_resp);
    std::cout << "\n Unpack router response time ";
    PrintTime(unpack_router_resp);
    /*for(int i = 0; i < number_of_bucket_servers; i++)
      {
      std::cout << global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].idle_util/util_requests << ",";
      }*/
    std::cout << std::endl;
}

void PrintLatency(const GlobalStats &global_stats,
        const unsigned int number_of_lookup_servers,
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
        const unsigned int number_of_lookup_servers,
        const unsigned int util_requests)
{
    std::cout << (global_stats.percent_util_info.router_util_percent.user_util/util_requests) + (global_stats.percent_util_info.router_util_percent.system_util/util_requests) + (global_stats.percent_util_info.router_util_percent.io_util/util_requests) << " ";
    std::cout.flush();
    for(unsigned int i = 0; i < number_of_lookup_servers; i++)
    {
        std::cout << (global_stats.percent_util_info.lookup_srv_util_percent[i].user_util/util_requests) + (global_stats.percent_util_info.lookup_srv_util_percent[i].system_util/util_requests) + (global_stats.percent_util_info.lookup_srv_util_percent[i].io_util/util_requests) << " ";
        std::cout.flush();
    }
}


void PrintTimingHistogram(std::vector<uint64_t> &time_vec)
{
    std::sort(time_vec.begin(), time_vec.end());
    uint64_t size = time_vec.size();

    std::cout << (float)time_vec[0.1 * size]/1000.0 << " " << (float)time_vec[0.2 * size]/1000.0 << " " << (float)time_vec[0.3 * size]/1000.0 << " " << (float)time_vec[0.4 * size]/1000.0 << " " << (float)time_vec[0.5 * size]/1000.0 << " " << (float)time_vec[0.6 * size]/1000.0 << " " << (float)time_vec[0.7 * size]/1000.0 << " " << (float)time_vec[0.8 * size]/1000.0 << " " << (float)time_vec[0.9 * size]/1000.0 << " " << (float)time_vec[0.99 * size]/1000.0 << " ";
}

void ResetMetaStats(GlobalStats* meta_stats,
        int number_of_lookup_servers)
{
    meta_stats->timing_info.clear();
    meta_stats->percent_util_info.router_util_percent.user_util = 0;
    meta_stats->percent_util_info.router_util_percent.system_util = 0;
    meta_stats->percent_util_info.router_util_percent.io_util = 0;
    meta_stats->percent_util_info.router_util_percent.idle_util = 0;
    for(int i = 0; i < number_of_lookup_servers; i++)
    {
        meta_stats->percent_util_info.lookup_srv_util_percent[i].user_util = 0;
        meta_stats->percent_util_info.lookup_srv_util_percent[i].system_util = 0;
        meta_stats->percent_util_info.lookup_srv_util_percent[i].io_util = 0;
        meta_stats->percent_util_info.lookup_srv_util_percent[i].idle_util = 0;
    }
}
