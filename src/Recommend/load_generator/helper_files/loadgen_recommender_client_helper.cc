#include "loadgen_recommender_client_helper.h"

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
        CHECK(false, "Format: ./<loadgen_recommender_client> <queries file path> <result file path> <Time to run the program> <QPS> <IP to bind to>\n");
    }
    return load_gen_command_line_args;
}

void CreateQueriesFromFile(std::string queries_file_name,
        std::vector<std::pair<int, int> >* queries)
{
    std::ifstream file(queries_file_name);
    CHECK((file.good()), "Cannot create queries from file because file does not exist\n");
    std::string line;
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buf(line);
        std::istream_iterator<std::string> begin(buf), end;
        std::vector<std::string> tokens(begin, end);

        queries->emplace_back(std::pair<int, int>());
        int itr = 0, first = 0, second = 0;
        for(auto& s: tokens)
        {
            if (itr > 1) {
                continue;
            }
            if (itr == 0) {
                first = std::stoul(s, nullptr, 0);
            } else {
                second = std::stoul(s, nullptr, 0);
            }
            itr++;
        }
        queries->back() = std::make_pair(first, second);
    }
}

void CreateRecommenderServiceRequest(const std::pair<int, int> query,
        const bool util_request,
        recommender::RecommenderRequest* recommender_request)
{
    recommender_request->set_user(std::get<0>(query));
    recommender_request->set_item(std::get<1>(query));
    recommender_request->mutable_util_request()->set_util_request(util_request);
}

void UnpackRecommenderServiceResponse(const recommender::RecommenderResponse &recommender_reply,
        float* rating,
        TimingInfo* timing_info,                                                    
        UtilInfo* previous_util,                                                                                                                                                                                                                                                                                                                                                  PercentUtilInfo* percent_util_info)
{
    *rating = recommender_reply.rating();
    UnpackTimingInfo(recommender_reply, timing_info);
    UnpackUtilInfo(recommender_reply, previous_util, percent_util_info);
}

/* Following two functions are helpers to the above function:
   They unpack stats.*/
void UnpackTimingInfo(const recommender::RecommenderResponse &recommender_reply,
        TimingInfo* timing_info)
{
    timing_info->unpack_recommender_req_time = recommender_reply.unpack_recommender_req_time();
    timing_info->create_cf_srv_req_time = recommender_reply.create_cf_srv_req_time();
    timing_info->unpack_cf_srv_resp_time = recommender_reply.unpack_cf_srv_resp_time();
    timing_info->pack_recommender_resp_time = recommender_reply.pack_recommender_resp_time();
    timing_info->unpack_cf_srv_req_time = recommender_reply.unpack_cf_srv_req_time();
    timing_info->calculate_cf_time = recommender_reply.calculate_cf_srv_time();
    timing_info->pack_cf_srv_resp_time = recommender_reply.pack_cf_srv_resp_time();
    timing_info->recommender_time = recommender_reply.recommender_time();
}

void UnpackUtilInfo(const recommender::RecommenderResponse &recommender_reply,
        UtilInfo* util,
        PercentUtilInfo* percent_util_info)
{
    if (recommender_reply.util_response().util_present() == false) {
        util->util_info_present = false;
        return;
    }
    util->util_info_present = true;
    /*If util exists, we need to get the cf_srv and recommender utils.
      In order to do so, we must first
      get the deltas between the previous and current
      times. Getting recommender first.*/
    uint64_t recommender_user_time_delta = recommender_reply.util_response().recommender_util().user_time() - util->recommender_util.user_time;
    uint64_t recommender_system_time_delta = recommender_reply.util_response().recommender_util().system_time() - util->recommender_util.system_time;
    uint64_t recommender_io_time_delta = recommender_reply.util_response().recommender_util().io_time() - util->recommender_util.io_time;
    uint64_t recommender_idle_time_delta = recommender_reply.util_response().recommender_util().idle_time() - util->recommender_util.idle_time;

    /* We then get the total, to compute util as a fraction
       of the total time.*/
    uint64_t total_recommender_time_delta = recommender_user_time_delta + recommender_system_time_delta + recommender_io_time_delta + recommender_idle_time_delta;
    percent_util_info->recommender_util_percent.user_util = 100.0 * ((float)(recommender_user_time_delta)/(float)(total_recommender_time_delta));
    percent_util_info->recommender_util_percent.system_util = 100.0 * ((float)(recommender_system_time_delta)/(float)(total_recommender_time_delta));
    percent_util_info->recommender_util_percent.io_util = 100.0 * ((float)(recommender_io_time_delta)/(float)(total_recommender_time_delta));
    percent_util_info->recommender_util_percent.idle_util = 100.0 * ((float)(recommender_idle_time_delta)/(float)(total_recommender_time_delta));


    //std::cout << "recommender user = " << percent_util_info->recommender_util_percent.user_util << "recommender system = " << percent_util_info->recommender_util_percent.system_util << "recommender io = " << percent_util_info->recommender_util_percent.io_util << "recommender idle = " << percent_util_info->recommender_util_percent.idle_util << std::endl;

    /*Update the previous value as the value obtained from the
      response, so that we are ready for the next round.*/
    util->recommender_util.user_time = recommender_reply.util_response().recommender_util().user_time();
    util->recommender_util.system_time = recommender_reply.util_response().recommender_util().system_time();
    util->recommender_util.io_time = recommender_reply.util_response().recommender_util().io_time();
    util->recommender_util.idle_time = recommender_reply.util_response().recommender_util().idle_time();
    /* Buckets utils are then obtained for every
       cf_srv server.*/
    for(int i = 0; i < recommender_reply.util_response().cf_srv_util_size(); i++)
    {
        uint64_t cf_srv_user_time_delta = recommender_reply.util_response().cf_srv_util(i).user_time() - util->cf_srv_util[i].user_time;
        uint64_t cf_srv_system_time_delta = recommender_reply.util_response().cf_srv_util(i).system_time() - util->cf_srv_util[i].system_time;
        uint64_t cf_srv_io_time_delta = recommender_reply.util_response().cf_srv_util(i).io_time() - util->cf_srv_util[i].io_time;
        uint64_t cf_srv_idle_time_delta = recommender_reply.util_response().cf_srv_util(i).idle_time() - util->cf_srv_util[i].idle_time;
        uint64_t total_cf_srv_time_delta = cf_srv_user_time_delta + cf_srv_system_time_delta + cf_srv_io_time_delta + cf_srv_idle_time_delta;
        percent_util_info->cf_srv_util_percent[i].user_util = 100.0 * ((float)(cf_srv_user_time_delta)/(float)(total_cf_srv_time_delta));
        percent_util_info->cf_srv_util_percent[i].system_util = 100.0 * ((float)(cf_srv_system_time_delta)/(float)(total_cf_srv_time_delta));
        percent_util_info->cf_srv_util_percent[i].io_util = 100.0 * ((float)(cf_srv_io_time_delta)/(float)(total_cf_srv_time_delta));
        percent_util_info->cf_srv_util_percent[i].idle_util = 100.0 * ((float)(cf_srv_idle_time_delta)/(float)(total_cf_srv_time_delta));

        //std::cout << "cf_srv user = " << percent_util_info->cf_srv_util_percent[i].user_util << "cf_srv system = " << percent_util_info->cf_srv_util_percent[i].system_util << "cf_srv io = " << percent_util_info->cf_srv_util_percent[i].io_util << "cf_srv idle = " << percent_util_info->cf_srv_util_percent[i].idle_util << std::endl;
        util->cf_srv_util[i].user_time = recommender_reply.util_response().cf_srv_util(i).user_time();
        util->cf_srv_util[i].system_time = recommender_reply.util_response().cf_srv_util(i).system_time();
        util->cf_srv_util[i].io_time = recommender_reply.util_response().cf_srv_util(i).io_time();
        util->cf_srv_util[i].idle_time = recommender_reply.util_response().cf_srv_util(i).idle_time();
    }
}

void PrintValueForAllQueries(const float rating)
{
    std::cout << rating << std::endl;
}

void UpdateGlobalTimingStats(const TimingInfo &timing_info,
        GlobalStats* global_stats)
{
    /*global_stats->timing_info.create_recommender_req_time += timing_info.create_recommender_req_time;
      global_stats->timing_info.unpack_loadgen_req_time += timing_info.unpack_loadgen_req_time;
      global_stats->timing_info.get_point_ids_time += timing_info.get_point_ids_time;
      global_stats->timing_info.create_bucket_req_time += timing_info.create_bucket_req_time;
      global_stats->timing_info.unpack_bucket_req_time += timing_info.unpack_bucket_req_time;
      global_stats->timing_info.calculate_knn_time += timing_info.calculate_knn_time;
      global_stats->timing_info.pack_bucket_resp_time += timing_info.pack_bucket_resp_time;
      global_stats->timing_info.unpack_bucket_resp_time += timing_info.unpack_bucket_resp_time;
      global_stats->timing_info.merge_time += timing_info.merge_time;
      global_stats->timing_info.pack_recommender_resp_time += timing_info.pack_recommender_resp_time;
      global_stats->timing_info.unpack_recommender_resp_time += timing_info.unpack_recommender_resp_time;
      global_stats->timing_info.update_recommender_util_time += timing_info.update_recommender_util_time;
      global_stats->timing_info.get_bucket_responses_time += timing_info.get_bucket_responses_time;
      global_stats->timing_info.total_resp_time += timing_info.total_resp_time;*/
    global_stats->timing_info.push_back(timing_info);
}

void UpdateGlobalUtilStats(PercentUtilInfo* percent_util_info,
        const unsigned int number_of_cf_servers,
        GlobalStats* global_stats)
{
    global_stats->percent_util_info.recommender_util_percent.user_util += percent_util_info->recommender_util_percent.user_util;
    global_stats->percent_util_info.recommender_util_percent.system_util += percent_util_info->recommender_util_percent.system_util;
    global_stats->percent_util_info.recommender_util_percent.io_util += percent_util_info->recommender_util_percent.io_util;
    global_stats->percent_util_info.recommender_util_percent.idle_util += percent_util_info->recommender_util_percent.idle_util;
    for(unsigned int i = 0; i < number_of_cf_servers; i++)
    {
        global_stats->percent_util_info.cf_srv_util_percent[i].user_util += percent_util_info->cf_srv_util_percent[i].user_util;
        global_stats->percent_util_info.cf_srv_util_percent[i].system_util += percent_util_info->cf_srv_util_percent[i].system_util;
        global_stats->percent_util_info.cf_srv_util_percent[i].io_util += percent_util_info->cf_srv_util_percent[i].io_util;
        global_stats->percent_util_info.cf_srv_util_percent[i].idle_util += percent_util_info->cf_srv_util_percent[i].idle_util;
    }
}

void PrintTime(std::vector<uint64_t> time_vec)
{
    uint64_t size = time_vec.size();
    std::cout << (float)time_vec[0.1*size]/1000.0 << " " << (float)time_vec[0.2*size]/1000.0 << " " << (float)time_vec[0.3*size]/1000.0 << " " << (float)time_vec[0.4*size]/1000.0 << " " << (float)time_vec[0.5*size]/1000.0 << " " << (float)time_vec[0.6*size]/1000.0 << " " << (float)time_vec[0.7*size]/1000.0 << " " << (float)time_vec[0.8*size]/1000.0 << " " << (float)time_vec[0.9*size]/1000.0 << " " << (float)(float)time_vec[0.95*size]/1000.0 << " " << (float)(float)time_vec[0.99*size]/1000.0 << " " << (float)(float)time_vec[0.999*size]/1000.0 << " ";
}

float ComputeQueryCost(const GlobalStats &global_stats,
        const unsigned int util_requests,
        const unsigned int number_of_cf_servers,
        float achieved_qps)
{
    // First add up all the recommender utils - this is % data
    unsigned int total_utilization = (global_stats.percent_util_info.recommender_util_percent.user_util/util_requests) + (global_stats.percent_util_info.recommender_util_percent.system_util/util_requests) + (global_stats.percent_util_info.recommender_util_percent.io_util/util_requests);
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
        const unsigned int number_of_cf_servers,
        const unsigned int util_requests,
        const unsigned int responses_recvd)
{
    /*std::cout << global_stats.timing_info.create_recommender_req_time/responses_recvd << "," << global_stats.timing_info.update_recommender_util_time/responses_recvd << "," << global_stats.timing_info.unpack_loadgen_req_time/responses_recvd << "," << global_stats.timing_info.get_point_ids_time/responses_recvd << "," << global_stats.timing_info.get_bucket_responses_time/responses_recvd << "," << global_stats.timing_info.create_bucket_req_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_req_time/responses_recvd << "," << global_stats.timing_info.calculate_knn_time/responses_recvd << "," << global_stats.timing_info.pack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_bucket_resp_time/responses_recvd << "," << global_stats.timing_info.merge_time/responses_recvd << "," << global_stats.timing_info.pack_recommender_resp_time/responses_recvd << "," << global_stats.timing_info.unpack_recommender_resp_time/responses_recvd << "," << global_stats.timing_info.total_resp_time/responses_recvd << "," << global_stats.percent_util_info.recommender_util_percent.user_util/util_requests << "," << global_stats.percent_util_info.recommender_util_percent.system_util/util_requests << "," << global_stats.percent_util_info.recommender_util_percent.io_util/util_requests << "," << global_stats.percent_util_info.recommender_util_percent.idle_util/util_requests << ",";*/

    std::vector<uint64_t> total_response_time, create_recommender_req, update_recommender_util, unpack_recommender_req, get_cf_srv_responses, create_cf_srv_req, unpack_cf_srv_req, calculate_cf_time, pack_cf_srv_resp, unpack_cf_srv_resp, merge, pack_recommender_resp, unpack_recommender_resp, recommender_time;
    unsigned int timing_info_size = global_stats.timing_info.size();
    for(unsigned int i = 0; i < timing_info_size; i++)
    {
        total_response_time.push_back(global_stats.timing_info[i].total_resp_time);
        create_recommender_req.push_back(global_stats.timing_info[i].create_recommender_req_time);
        update_recommender_util.push_back(global_stats.timing_info[i].update_recommender_util_time);
        unpack_recommender_req.push_back(global_stats.timing_info[i].unpack_recommender_req_time);
        get_cf_srv_responses.push_back(global_stats.timing_info[i].get_cf_srv_responses_time);
        create_cf_srv_req.push_back(global_stats.timing_info[i].create_cf_srv_req_time);
        unpack_cf_srv_req.push_back(global_stats.timing_info[i].unpack_cf_srv_req_time);
        calculate_cf_time.push_back(global_stats.timing_info[i].calculate_cf_time);
        pack_cf_srv_resp.push_back(global_stats.timing_info[i].pack_cf_srv_resp_time);
        unpack_cf_srv_resp.push_back(global_stats.timing_info[i].unpack_cf_srv_resp_time);
        pack_recommender_resp.push_back(global_stats.timing_info[i].pack_recommender_resp_time);
        unpack_recommender_resp.push_back(global_stats.timing_info[i].unpack_recommender_resp_time);
        recommender_time.push_back(global_stats.timing_info[i].recommender_time);
    }
    std::sort(total_response_time.begin(), total_response_time.end());
    std::sort(create_recommender_req.begin(), create_recommender_req.end());
    std::sort(update_recommender_util.begin(), update_recommender_util.end());
    std::sort(unpack_recommender_req.begin(), unpack_recommender_req.end());
    std::sort(get_cf_srv_responses.begin(), get_cf_srv_responses.end());
    std::sort(create_cf_srv_req.begin(), create_cf_srv_req.end());
    std::sort(unpack_cf_srv_req.begin(), unpack_cf_srv_req.end());
    std::sort(calculate_cf_time.begin(), calculate_cf_time.end());
    std::sort(pack_cf_srv_resp.begin(), pack_cf_srv_resp.end());
    std::sort(unpack_cf_srv_resp.begin(), unpack_cf_srv_resp.end());
    std::sort(pack_recommender_resp.begin(), pack_recommender_resp.end());
    std::sort(unpack_recommender_resp.begin(), unpack_recommender_resp.end());
    std::sort(recommender_time.begin(), recommender_time.end());
    std::cout << "\n Total response time \n"; 
    PrintTime(total_response_time);
    std::cout << std::endl;
    std::cout << "\n Index creation time ";
    PrintTime(create_recommender_req);
    std::cout << "\n Update recommender util time ";
    PrintTime(update_recommender_util);
    std::cout << "\n Unpack loadgen request time ";
    PrintTime(unpack_recommender_req);
    std::cout << "\n Total time taken by recommender server: \n";
    PrintTime(recommender_time);
    //std::cout << std::endl;
    std::cout << "\n Get bucket responses time \n";
    PrintTime(get_cf_srv_responses);
    std::cout << "\n Create bucket request time ";
    PrintTime(create_cf_srv_req);
    std::cout << "\n Unpack bucket request time ";
    PrintTime(unpack_cf_srv_req);
    std::cout << "\n Calculate knn time \n";
    PrintTime(calculate_cf_time);
    std::cout << "\n Pack bucket response time ";
    PrintTime(pack_cf_srv_resp);
    std::cout << "\n Unpack bucket response time ";
    PrintTime(unpack_cf_srv_resp);
    std::cout << "\n Pack recommender response time ";
    PrintTime(pack_recommender_resp);
    std::cout << "\n Unpack recommender response time ";
    PrintTime(unpack_recommender_resp);
    /*for(int i = 0; i < number_of_bucket_servers; i++)
      {
      std::cout << global_stats.percent_util_info.bucket_util_percent[i].user_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].system_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].io_util/util_requests << "," << global_stats.percent_util_info.bucket_util_percent[i].idle_util/util_requests << ",";
      }*/
    std::cout << std::endl;
}

void PrintLatency(const GlobalStats &global_stats,
        const unsigned int number_of_cf_servers,
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
        const unsigned int number_of_cf_servers,
        const unsigned int util_requests)
{
    std::cout << (global_stats.percent_util_info.recommender_util_percent.user_util/util_requests) + (global_stats.percent_util_info.recommender_util_percent.system_util/util_requests) + (global_stats.percent_util_info.recommender_util_percent.io_util/util_requests) << " ";
    std::cout.flush();
    for(unsigned int i = 0; i < number_of_cf_servers; i++)
    {
        std::cout << (global_stats.percent_util_info.cf_srv_util_percent[i].user_util/util_requests) + (global_stats.percent_util_info.cf_srv_util_percent[i].system_util/util_requests) + (global_stats.percent_util_info.cf_srv_util_percent[i].io_util/util_requests) << " ";
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
        int number_of_cf_servers)
{
    meta_stats->timing_info.clear();
    meta_stats->percent_util_info.recommender_util_percent.user_util = 0;
    meta_stats->percent_util_info.recommender_util_percent.system_util = 0;
    meta_stats->percent_util_info.recommender_util_percent.io_util = 0;
    meta_stats->percent_util_info.recommender_util_percent.idle_util = 0;
    for(int i = 0; i < number_of_cf_servers; i++)
    {
        meta_stats->percent_util_info.cf_srv_util_percent[i].user_util = 0;
        meta_stats->percent_util_info.cf_srv_util_percent[i].system_util = 0;
        meta_stats->percent_util_info.cf_srv_util_percent[i].io_util = 0;
        meta_stats->percent_util_info.cf_srv_util_percent[i].idle_util = 0;
    }
}
