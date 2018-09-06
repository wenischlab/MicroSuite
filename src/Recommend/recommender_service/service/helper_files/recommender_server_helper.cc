#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <unordered_map>
#include "recommender_server_helper.h"

void GetCFServerIPs(const std::string &cf_server_ips_file,
        std::vector<std::string>* cf_server_ips)
{
    std::ifstream file(cf_server_ips_file);
    CHECK((file.good()), "ERROR: File containing cf server IPs must exists\n");
    std::string line = "";
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buffer(line);
        std::istream_iterator<std::string> begin(buffer), end;
        std::vector<std::string> tokens(begin, end);
        /* Tokens must contain only one IP -
           Each IP address must be on a different line.*/
        CHECK((tokens.size() == 1), "ERROR: File must contain only one IP address per line\n");
        cf_server_ips->push_back(tokens[0]);
    }
}

void UnpackRecommenderServiceRequest(const recommender::RecommenderRequest &recommender_request,
        int* user,
        int* item,
        collaborative_filtering::CFRequest* request_to_cf_srv)
{
    *user = recommender_request.user();
    *item = recommender_request.item();
    request_to_cf_srv->set_user(*user);
    request_to_cf_srv->set_item(*item);
}

void Merge(struct ThreadArgs* thread_args,
        unsigned int number_of_cf_servers,
        float* final_rating,
        uint64_t* create_cf_srv_req_time,
        uint64_t* unpack_cf_srv_resp_time,
        uint64_t* unpack_cf_srv_req_time,
        uint64_t* cf_srv_time,
        uint64_t* pack_cf_srv_resp_time,
        recommender::RecommenderResponse* recommender_reply)
{
    *final_rating = 0;
    for(unsigned int j = 0; j < number_of_cf_servers; j++) {
        *final_rating = *final_rating + thread_args[j].rating;

        (*create_cf_srv_req_time) += thread_args[j].cf_srv_timing_info.create_cf_srv_request_time;
        (*unpack_cf_srv_resp_time) += thread_args[j].cf_srv_timing_info.unpack_cf_srv_resp_time;
        (*unpack_cf_srv_req_time) += thread_args[j].cf_srv_timing_info.unpack_cf_srv_req_time;
        (*cf_srv_time) += thread_args[j].cf_srv_timing_info.cf_srv_time;
        (*pack_cf_srv_resp_time) += thread_args[j].cf_srv_timing_info.pack_cf_srv_resp_time;
    }
    *final_rating = *final_rating/number_of_cf_servers;
    recommender_reply->set_rating(*final_rating);

    (*create_cf_srv_req_time) = (*create_cf_srv_req_time)/number_of_cf_servers;
    (*unpack_cf_srv_resp_time) = (*unpack_cf_srv_resp_time)/number_of_cf_servers;
    (*unpack_cf_srv_req_time) = (*unpack_cf_srv_req_time)/number_of_cf_servers;
    (*cf_srv_time) = (*cf_srv_time)/number_of_cf_servers;
    (*pack_cf_srv_resp_time) = (*pack_cf_srv_resp_time)/number_of_cf_servers;

    recommender_reply->set_create_cf_srv_req_time(*create_cf_srv_req_time);
    recommender_reply->set_unpack_cf_srv_resp_time(*unpack_cf_srv_resp_time);
    recommender_reply->set_unpack_cf_srv_req_time(*unpack_cf_srv_req_time);
    recommender_reply->set_calculate_cf_srv_time(*cf_srv_time);
    recommender_reply->set_pack_cf_srv_resp_time(*pack_cf_srv_resp_time);
    recommender_reply->set_number_of_cf_servers(number_of_cf_servers);
}

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const int number_of_cf_servers,
        recommender::RecommenderResponse* recommender_reply)
{
    uint64_t create_cf_srv_req_time = 0, unpack_cf_srv_resp_time = 0, unpack_cf_srv_req_time = 0, cf_srv_time = 0, pack_cf_srv_resp_time = 0;
    float final_rating = 0.0;

    for(int i = 0; i < number_of_cf_servers; i++) {
        final_rating += *(response_data[i].rating);
        create_cf_srv_req_time += response_data[i].cf_srv_timing_info->create_cf_srv_request_time;
        unpack_cf_srv_resp_time += response_data[i].cf_srv_timing_info->unpack_cf_srv_resp_time;
        unpack_cf_srv_req_time += response_data[i].cf_srv_timing_info->unpack_cf_srv_req_time;
        cf_srv_time += response_data[i].cf_srv_timing_info->cf_srv_time;
        pack_cf_srv_resp_time += response_data[i].cf_srv_timing_info->pack_cf_srv_resp_time;
    }
    final_rating = final_rating / number_of_cf_servers;
    recommender_reply->set_rating(final_rating);

    create_cf_srv_req_time = create_cf_srv_req_time/number_of_cf_servers;
    unpack_cf_srv_resp_time = unpack_cf_srv_resp_time/number_of_cf_servers;
    unpack_cf_srv_req_time = unpack_cf_srv_req_time/number_of_cf_servers;
    cf_srv_time = cf_srv_time/number_of_cf_servers;
    pack_cf_srv_resp_time = pack_cf_srv_resp_time/number_of_cf_servers;

    recommender_reply->set_create_cf_srv_req_time(create_cf_srv_req_time);
    recommender_reply->set_unpack_cf_srv_resp_time(unpack_cf_srv_resp_time);
    recommender_reply->set_unpack_cf_srv_req_time(unpack_cf_srv_req_time);
    recommender_reply->set_calculate_cf_srv_time(cf_srv_time);
    recommender_reply->set_pack_cf_srv_resp_time(pack_cf_srv_resp_time);
    recommender_reply->set_number_of_cf_servers(number_of_cf_servers);

    /* Pack util info for all bucket servers. We do not
       want the mean because we want to know how each bucket behaves i.e
       does one perform worse than the others / are they uniform? */
    for(int i = 0; i < number_of_cf_servers; i++)
    {
        recommender::Util* cf_srv_util = recommender_reply->mutable_util_response()->add_cf_srv_util();
        cf_srv_util->set_user_time(response_data[i].cf_srv_util->user_time);
        cf_srv_util->set_system_time(response_data[i].cf_srv_util->system_time);
        cf_srv_util->set_io_time(response_data[i].cf_srv_util->io_time);
        cf_srv_util->set_idle_time(response_data[i].cf_srv_util->idle_time);
    }
}

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
                    FMSyncTMNames tm_name = sdb20_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(20, 1, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 2:
                {
                    FMSyncTMNames tm_name = last_sync;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(1, 50, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
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
                    FMAsyncTMNames tm_name = last_async;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMAsyncTMNames, TMConfig>(tm_name, TMConfig(1, 4, 4)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
        }

    }
}
