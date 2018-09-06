#include "loadgen_union_client_helper.h"

LoadGenCommandLineArgs* ParseLoadGenCommandLine(const int &argc,
        char** argv)
{
    struct LoadGenCommandLineArgs* load_gen_command_line_args = new struct LoadGenCommandLineArgs();
    if (argc == 6) {
        try
        {
            load_gen_command_line_args->queries_file_name = argv[1];
            load_gen_command_line_args->result_file_name = argv[2];
            load_gen_command_line_args->time_duration = std::stoul(argv[3], nullptr, 0);
            load_gen_command_line_args->qps = std::atof(argv[4]);
            load_gen_command_line_args->ip = argv[5];
        }
        catch(...)
        {
            CHECK(false, "Enter a valid number for valid string for queries path/ time to run the program/ QPS/ IP to bind to\n");
        }
    } else {
        CHECK(false, "Format: ./<loadgen_union_client> <queries file path> <result file path> <Time to run the program> <QPS> <IP to bind to>\n");
    }
    return load_gen_command_line_args;
}

void CreateQueriesFromFile(std::string queries_file_name,
        std::vector<std::vector<Wordids> >* queries)
{
    std::ifstream file(queries_file_name);
    CHECK((file.good()), "Cannot create queries from file because file does not exist\n");
    std::string line;
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buf(line);
        std::istream_iterator<std::string> begin(buf), end;
        std::vector<std::string> tokens(begin, end);
        
        queries->emplace_back(std::vector<Wordids>());
        for(auto& s: tokens)
        {
            queries->back().emplace_back(std::stoul(s, nullptr, 0));
        }
    }
}

void CreateUnionServiceRequest(const std::vector<Wordids> query,
        const bool util_request,
        union_service::UnionRequest* union_request)
{
    int query_size = query.size();
    for(int i = 0; i < query_size; i++) {
        union_request->add_word_ids(query[i]);
    }
    union_request->mutable_util_request()->set_util_request(util_request);
}

void UnpackUnionServiceResponse(const union_service::UnionResponse &union_reply,
        std::vector<Docids>* posting_list,
        TimingInfo* timing_info,                                                    
        UtilInfo* previous_util,                                                                                                                                                                                                                                                                                                                                                  PercentUtilInfo* percent_util_info)
{
    Docids posting_list_size = union_reply.doc_ids_size();
    for(Docids i = 0; i < posting_list_size; i++)
    {
        posting_list->emplace_back(union_reply.doc_ids(i));
    }
    UnpackTimingInfo(union_reply, timing_info);
    UnpackUtilInfo(union_reply, previous_util, percent_util_info);
}

/* Following two functions are helpers to the above function:
   They unpack stats.*/
void UnpackTimingInfo(const union_service::UnionResponse &union_reply,
        TimingInfo* timing_info)
{
    timing_info->unpack_union_req_time = union_reply.unpack_union_req_time();
    timing_info->create_intersection_srv_req_time = union_reply.create_intersection_srv_req_time();
    timing_info->unpack_intersection_srv_resp_time = union_reply.unpack_intersection_srv_resp_time();
    timing_info->pack_union_resp_time = union_reply.pack_union_resp_time();
    timing_info->unpack_intersection_srv_req_time = union_reply.unpack_intersection_srv_req_time();
    timing_info->calculate_intersection_time = union_reply.calculate_intersection_time();
    timing_info->pack_intersection_srv_resp_time = union_reply.pack_intersection_srv_resp_time();
    timing_info->union_time = union_reply.union_time();
}

void UnpackUtilInfo(const union_service::UnionResponse &union_reply,
        UtilInfo* util,
        PercentUtilInfo* percent_util_info)
{
    if (union_reply.util_response().util_present() == false) {
        util->util_info_present = false;
        return;
    }
    util->util_info_present = true;
    /*If util exists, we need to get the intersection_srv and union utils.
      In order to do so, we must first
      get the deltas between the previous and current
      times. Getting union first.*/
    uint64_t union_user_time_delta = union_reply.util_response().union_util().user_time() - util->union_util.user_time;
    uint64_t union_system_time_delta = union_reply.util_response().union_util().system_time() - util->union_util.system_time;
    uint64_t union_io_time_delta = union_reply.util_response().union_util().io_time() - util->union_util.io_time;
    uint64_t union_idle_time_delta = union_reply.util_response().union_util().idle_time() - util->union_util.idle_time;

    /* We then get the total, to compute util as a fraction
       of the total time.*/
    uint64_t total_union_time_delta = union_user_time_delta + union_system_time_delta + union_io_time_delta + union_idle_time_delta;
    percent_util_info->union_util_percent.user_util = 100.0 * ((float)(union_user_time_delta)/(float)(total_union_time_delta));
    percent_util_info->union_util_percent.system_util = 100.0 * ((float)(union_system_time_delta)/(float)(total_union_time_delta));
    percent_util_info->union_util_percent.io_util = 100.0 * ((float)(union_io_time_delta)/(float)(total_union_time_delta));
    percent_util_info->union_util_percent.idle_util = 100.0 * ((float)(union_idle_time_delta)/(float)(total_union_time_delta));


    //std::cout << "union user = " << percent_util_info->union_util_percent.user_util << "union system = " << percent_util_info->union_util_percent.system_util << "union io = " << percent_util_info->union_util_percent.io_util << "union idle = " << percent_util_info->union_util_percent.idle_util << std::endl;

    /*Update the previous value as the value obtained from the
      response, so that we are ready for the next round.*/
    util->union_util.user_time = union_reply.util_response().union_util().user_time();
    util->union_util.system_time = union_reply.util_response().union_util().system_time();
    util->union_util.io_time = union_reply.util_response().union_util().io_time();
    util->union_util.idle_time = union_reply.util_response().union_util().idle_time();
    /* Buckets utils are then obtained for every
       intersection_srv server.*/
    for(int i = 0; i < union_reply.util_response().intersection_srv_util_size(); i++)
    {
        uint64_t intersection_srv_user_time_delta = union_reply.util_response().intersection_srv_util(i).user_time() - util->intersection_srv_util[i].user_time;
        uint64_t intersection_srv_system_time_delta = union_reply.util_response().intersection_srv_util(i).system_time() - util->intersection_srv_util[i].system_time;
        uint64_t intersection_srv_io_time_delta = union_reply.util_response().intersection_srv_util(i).io_time() - util->intersection_srv_util[i].io_time;
        uint64_t intersection_srv_idle_time_delta = union_reply.util_response().intersection_srv_util(i).idle_time() - util->intersection_srv_util[i].idle_time;
        uint64_t total_intersection_srv_time_delta = intersection_srv_user_time_delta + intersection_srv_system_time_delta + intersection_srv_io_time_delta + intersection_srv_idle_time_delta;
        percent_util_info->intersection_srv_util_percent[i].user_util = 100.0 * ((float)(intersection_srv_user_time_delta)/(float)(total_intersection_srv_time_delta));
        percent_util_info->intersection_srv_util_percent[i].system_util = 100.0 * ((float)(intersection_srv_system_time_delta)/(float)(total_intersection_srv_time_delta));
        percent_util_info->intersection_srv_util_percent[i].io_util = 100.0 * ((float)(intersection_srv_io_time_delta)/(float)(total_intersection_srv_time_delta));
        percent_util_info->intersection_srv_util_percent[i].idle_util = 100.0 * ((float)(intersection_srv_idle_time_delta)/(float)(total_intersection_srv_time_delta));

        //std::cout << "intersection_srv user = " << percent_util_info->intersection_srv_util_percent[i].user_util << "intersection_srv system = " << percent_util_info->intersection_srv_util_percent[i].system_util << "intersection_srv io = " << percent_util_info->intersection_srv_util_percent[i].io_util << "intersection_srv idle = " << percent_util_info->intersection_srv_util_percent[i].idle_util << std::endl;
        util->intersection_srv_util[i].user_time = union_reply.util_response().intersection_srv_util(i).user_time();
        util->intersection_srv_util[i].system_time = union_reply.util_response().intersection_srv_util(i).system_time();
        util->intersection_srv_util[i].io_time = union_reply.util_response().intersection_srv_util(i).io_time();
        util->intersection_srv_util[i].idle_time = union_reply.util_response().intersection_srv_util(i).idle_time();
    }
}

void PrintValueForAllQueries(const std::vector<Docids> posting_list)
{
    Docids posting_list_size = posting_list.size();
    for(Docids i = 0; i < posting_list_size; i++)
    {
        std::cout << posting_list[i] << std::endl;
    }
}

void UpdateGlobalTimingStats(const TimingInfo &timing_info,
        GlobalStats* global_stats)
{
    /*global_stats->timing_info.create_union_req_time += timing_info.create_union_req_time;
      global_stats->timing_info.unpack_loadgen_req_time += timing_info.unpack_loadgen_req_time;
      global_stats->timing_info.get_point_ids_time += timing_info.get_point_ids_time;
      global_stats->timing_info.create_bucket_req_time += timing_info.create_bucket_req_time;
      global_stats->timing_info.unpack_bucket_req_time += timing_info.unpack_bucket_req_time;
      global_stats->timing_info.calculate_knn_time += timing_info.calculate_knn_time;
      global_stats->timing_info.pack_bucket_resp_time += timing_info.pack_bucket_resp_time;
      global_stats->timing_info.unpack_bucket_resp_time += timing_info.unpack_bucket_resp_time;
      global_stats->timing_info.merge_time += timing_info.merge_time;
      global_stats->timing_info.pack_union_resp_time += timing_info.pack_union_resp_time;
      global_stats->timing_info.unpack_union_resp_time += timing_info.unpack_union_resp_time;
      global_stats->timing_info.update_union_util_time += timing_info.update_union_util_time;
      global_stats->timing_info.get_bucket_responses_time += timing_info.get_bucket_responses_time;
      global_stats->timing_info.total_resp_time += timing_info.total_resp_time;*/
    global_stats->timing_info.push_back(timing_info);
}

void UpdateGlobalUtilStats(PercentUtilInfo* percent_util_info,
        const unsigned int number_of_intersection_servers,
        GlobalStats* global_stats)
{
    global_stats->percent_util_info.union_util_percent.user_util += percent_util_info->union_util_percent.user_util;
    global_stats->percent_util_info.union_util_percent.system_util += percent_util_info->union_util_percent.system_util;
    global_stats->percent_util_info.union_util_percent.io_util += percent_util_info->union_util_percent.io_util;
    global_stats->percent_util_info.union_util_percent.idle_util += percent_util_info->union_util_percent.idle_util;
    for(unsigned int i = 0; i < number_of_intersection_servers; i++)
    {
        global_stats->percent_util_info.intersection_srv_util_percent[i].user_util += percent_util_info->intersection_srv_util_percent[i].user_util;
        global_stats->percent_util_info.intersection_srv_util_percent[i].system_util += percent_util_info->intersection_srv_util_percent[i].system_util;
        global_stats->percent_util_info.intersection_srv_util_percent[i].io_util += percent_util_info->intersection_srv_util_percent[i].io_util;
        global_stats->percent_util_info.intersection_srv_util_percent[i].idle_util += percent_util_info->intersection_srv_util_percent[i].idle_util;
    }
}

void PrintTime(std::vector<uint64_t> time_vec)
{
    uint64_t size = time_vec.size();
    std::cout << (float)time_vec[0.1*size]/1000.0 << " " << (float)time_vec[0.2*size]/1000.0 << " " << (float)time_vec[0.3*size]/1000.0 << " " << (float)time_vec[0.4*size]/1000.0 << " " << (float)time_vec[0.5*size]/1000.0 << " " << (float)time_vec[0.6*size]/1000.0 << " " << (float)time_vec[0.7*size]/1000.0 << " " << (float)time_vec[0.8*size]/1000.0 << " " << (float)time_vec[0.9*size]/1000.0 << " " << (float)(float)time_vec[0.95*size]/1000.0 << " " << (float)(float)time_vec[0.99*size]/1000.0 << " " << (float)(float)time_vec[0.999*size]/1000.0 << " ";
}

float ComputeQueryCost(const GlobalStats &global_stats,
        const unsigned int util_requests,
        const unsigned int number_of_intersection_servers,
        float achieved_qps)
{
    // First add up all the union utils - this is % data
    unsigned int total_utilization = (global_stats.percent_util_info.union_util_percent.user_util/util_requests) + (global_stats.percent_util_info.union_util_percent.system_util/util_requests) + (global_stats.percent_util_info.union_util_percent.io_util/util_requests);
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
        const unsigned int number_of_intersection_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd)
{
    /*std::cout << global_stats.timing_info.create_union_req_time/responses_recvd << "," << global_stats.timing_info.update_union_util_time/responses_recvd << "," << global_stats.timing_info.unpack_loadgen_req_time/responses_recvd << "," << global_stats.timing_info.get_point_ids_time/responses_recvd << "," << global_stats.timing_info.get_bucket_responses_time/responses_recvd << "," << global_stats.timing_info.create_bucket_req_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_req_time/responses_recvd << "," << global_stats.timing_info.calculate_knn_time/responses_recvd << "," << global_stats.timing_info.pack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.merge_time/responses_recvd << "," << global_stats.timing_info.pack_union_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_union_resp_time/responses_recvd << "," << global_stats.timing_info.total_resp_time/responses_recvd << "," << global_stats.percent_util_info.union_util_percent.user_util/util_requests << "," << global_stats.percent_util_info.union_util_percent.system_util/util_requests << "," << global_stats.percent_util_info.union_util_percent.io_util/util_requests << "," << global_stats.percent_util_info.union_util_percent.idle_util/util_requests << ",";*/

    std::vector<uint64_t> total_response_time, create_union_req, update_union_util, unpack_union_req, get_intersection_srv_responses, create_intersection_srv_req, unpack_intersection_srv_req, calculate_intersection_time, pack_intersection_srv_resp, unpack_intersection_srv_resp, merge, pack_union_resp, unpack_union_resp, union_time;
    unsigned int timing_info_size = global_stats.timing_info.size();
    for(unsigned int i = 0; i < timing_info_size; i++)
    {
        total_response_time.push_back(global_stats.timing_info[i].total_resp_time);
        create_union_req.push_back(global_stats.timing_info[i].create_union_req_time);
        update_union_util.push_back(global_stats.timing_info[i].update_union_util_time);
        unpack_union_req.push_back(global_stats.timing_info[i].unpack_union_req_time);
        get_intersection_srv_responses.push_back(global_stats.timing_info[i].get_intersection_srv_responses_time);
        create_intersection_srv_req.push_back(global_stats.timing_info[i].create_intersection_srv_req_time);
        unpack_intersection_srv_req.push_back(global_stats.timing_info[i].unpack_intersection_srv_req_time);
        calculate_intersection_time.push_back(global_stats.timing_info[i].calculate_intersection_time);
        pack_intersection_srv_resp.push_back(global_stats.timing_info[i].pack_intersection_srv_resp_time);
        unpack_intersection_srv_resp.push_back(global_stats.timing_info[i].unpack_intersection_srv_resp_time);
        pack_union_resp.push_back(global_stats.timing_info[i].pack_union_resp_time);
        unpack_union_resp.push_back(global_stats.timing_info[i].unpack_union_resp_time);
        union_time.push_back(global_stats.timing_info[i].union_time);
    }
    std::sort(total_response_time.begin(), total_response_time.end());
    std::sort(create_union_req.begin(), create_union_req.end());
    std::sort(update_union_util.begin(), update_union_util.end());
    std::sort(unpack_union_req.begin(), unpack_union_req.end());
    std::sort(get_intersection_srv_responses.begin(), get_intersection_srv_responses.end());
    std::sort(create_intersection_srv_req.begin(), create_intersection_srv_req.end());
    std::sort(unpack_intersection_srv_req.begin(), unpack_intersection_srv_req.end());
    std::sort(calculate_intersection_time.begin(), calculate_intersection_time.end());
    std::sort(pack_intersection_srv_resp.begin(), pack_intersection_srv_resp.end());
    std::sort(unpack_intersection_srv_resp.begin(), unpack_intersection_srv_resp.end());
    std::sort(pack_union_resp.begin(), pack_union_resp.end());
    std::sort(unpack_union_resp.begin(), unpack_union_resp.end());
    std::sort(union_time.begin(), union_time.end());
    std::cout << "\n Total response time \n"; 
    PrintTime(total_response_time);
    std::cout << std::endl;
    std::cout << "\n Index creation time ";
    PrintTime(create_union_req);
    std::cout << "\n Update union util time ";
    PrintTime(update_union_util);
    std::cout << "\n Unpack loadgen request time ";
    PrintTime(unpack_union_req);
    std::cout << "\n Total time taken by union server: \n";
    PrintTime(union_time);
    //std::cout << std::endl;
    std::cout << "\n Get bucket responses time \n";
    PrintTime(get_intersection_srv_responses);
    std::cout << "\n Create bucket request time ";
    PrintTime(create_intersection_srv_req);
    std::cout << "\n Unpack bucket request time ";
    PrintTime(unpack_intersection_srv_req);
    std::cout << "\n Calculate knn time \n";
    PrintTime(calculate_intersection_time);
    std::cout << "\n Pack bucket response time ";
    PrintTime(pack_intersection_srv_resp);
    std::cout << "\n Unpack bucket response time ";
    PrintTime(unpack_intersection_srv_resp);
    std::cout << "\n Pack union response time ";
    PrintTime(pack_union_resp);
    std::cout << "\n Unpack union response time ";
    PrintTime(unpack_union_resp);
    /*for(int i = 0; i < number_of_bucket_servers; i++)
      {
      std::cout << global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].idle_util/util_requests << ",";
      }*/
    std::cout << std::endl;
}

void PrintLatency(const GlobalStats &global_stats,
        const unsigned int number_of_intersection_servers,
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
        const unsigned int number_of_intersection_servers,
        const unsigned int util_requests)
{
    std::cout << (global_stats.percent_util_info.union_util_percent.user_util/util_requests) + (global_stats.percent_util_info.union_util_percent.system_util/util_requests) + (global_stats.percent_util_info.union_util_percent.io_util/util_requests) << " ";
    std::cout.flush();
    for(unsigned int i = 0; i < number_of_intersection_servers; i++)
    {
        std::cout << (global_stats.percent_util_info.intersection_srv_util_percent[i].user_util/util_requests) + (global_stats.percent_util_info.intersection_srv_util_percent[i].system_util/util_requests) + (global_stats.percent_util_info.intersection_srv_util_percent[i].io_util/util_requests) << " ";
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
        int number_of_intersection_servers)
{
    meta_stats->timing_info.clear();
    meta_stats->percent_util_info.union_util_percent.user_util = 0;
    meta_stats->percent_util_info.union_util_percent.system_util = 0;
    meta_stats->percent_util_info.union_util_percent.io_util = 0;
    meta_stats->percent_util_info.union_util_percent.idle_util = 0;
    for(int i = 0; i < number_of_intersection_servers; i++)
    {
        meta_stats->percent_util_info.intersection_srv_util_percent[i].user_util = 0;
        meta_stats->percent_util_info.intersection_srv_util_percent[i].system_util = 0;
        meta_stats->percent_util_info.intersection_srv_util_percent[i].io_util = 0;
        meta_stats->percent_util_info.intersection_srv_util_percent[i].idle_util = 0;
    }
}
