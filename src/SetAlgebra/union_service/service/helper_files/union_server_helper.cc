#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <unordered_map>
#include "union_server_helper.h"

void GetIntersectionServerIPs(const std::string &intersection_server_ips_file,
        std::vector<std::string>* intersection_server_ips)
{
    std::ifstream file(intersection_server_ips_file);
    CHECK((file.good()), "ERROR: File containing intersection server IPs must exists\n");
    std::string line = "";
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buffer(line);
        std::istream_iterator<std::string> begin(buffer), end;
        std::vector<std::string> tokens(begin, end);
        /* Tokens must contain only one IP -
           Each IP address must be on a different line.*/
        CHECK((tokens.size() == 1), "ERROR: File must contain only one IP address per line\n");
        intersection_server_ips->push_back(tokens[0]);
    }
}

void UnpackUnionServiceRequest(const union_service::UnionRequest &union_request,
        std::vector<Wordids>* word_ids,
        intersection::IntersectionRequest* request_to_intersection_srv)
{
    Wordids word_id_val = 0;
    Wordids num_ids = union_request.word_ids_size();

    for(unsigned int i = 0; i < num_ids; i++) {
        word_id_val = union_request.word_ids(i);
        word_ids->emplace_back(word_id_val);
        request_to_intersection_srv->add_word_ids(word_id_val);
    }
}

void Merge(const struct ThreadArgs* thread_args,
        const unsigned int number_of_intersection_servers,
        std::vector<Docids>* final_posting_list,
        uint64_t* create_intersection_srv_req_time,
        uint64_t* unpack_intersection_srv_resp_time,
        uint64_t* unpack_intersection_srv_req_time,
        uint64_t* calculate_intersection_time,
        uint64_t* pack_intersection_srv_resp_time)
{
    for(unsigned int j = 0; j < number_of_intersection_servers; j++)
    {
        (*create_intersection_srv_req_time) += thread_args[j].intersection_srv_timing_info.create_intersection_srv_request_time;
        (*unpack_intersection_srv_resp_time) += thread_args[j].intersection_srv_timing_info.unpack_intersection_srv_resp_time;
        (*unpack_intersection_srv_req_time) += thread_args[j].intersection_srv_timing_info.unpack_intersection_srv_req_time;
        (*calculate_intersection_time) += thread_args[j].intersection_srv_timing_info.calculate_intersection_time;
        (*pack_intersection_srv_resp_time) += thread_args[j].intersection_srv_timing_info.pack_intersection_srv_resp_time;
        for(unsigned int k = 0; k < thread_args[j].posting_list.size(); k++)
        {
            final_posting_list->emplace_back(thread_args[j].posting_list[k]);
        }
    }
    (*create_intersection_srv_req_time) = (*create_intersection_srv_req_time)/number_of_intersection_servers;
    (*unpack_intersection_srv_resp_time) = (*unpack_intersection_srv_resp_time)/number_of_intersection_servers;
    (*unpack_intersection_srv_req_time) = (*unpack_intersection_srv_req_time)/number_of_intersection_servers;
    (*calculate_intersection_time) = (*calculate_intersection_time)/number_of_intersection_servers;
    (*pack_intersection_srv_resp_time) = (*pack_intersection_srv_resp_time)/number_of_intersection_servers;
}

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const unsigned int number_of_intersection_servers,
        union_service::UnionResponse* union_reply)
{
    uint64_t create_intersection_srv_req_time = 0, unpack_intersection_srv_resp_time = 0, unpack_intersection_srv_req_time = 0, calculate_intersection_time = 0, pack_intersection_srv_resp_time = 0;
    for(unsigned int j = 0; j < number_of_intersection_servers; j++)
    {
        create_intersection_srv_req_time += response_data[j].intersection_srv_timing_info->create_intersection_srv_request_time;
        unpack_intersection_srv_resp_time += response_data[j].intersection_srv_timing_info->unpack_intersection_srv_resp_time;
        unpack_intersection_srv_req_time += response_data[j].intersection_srv_timing_info->unpack_intersection_srv_req_time;
        calculate_intersection_time += response_data[j].intersection_srv_timing_info->calculate_intersection_time;
        pack_intersection_srv_resp_time += response_data[j].intersection_srv_timing_info->pack_intersection_srv_resp_time;

        union_service::Util* intersection_srv_util = union_reply->mutable_util_response()->add_intersection_srv_util();
        intersection_srv_util->set_user_time(response_data[j].intersection_srv_util->user_time);
        intersection_srv_util->set_system_time(response_data[j].intersection_srv_util->system_time);
        intersection_srv_util->set_io_time(response_data[j].intersection_srv_util->io_time);
        intersection_srv_util->set_idle_time(response_data[j].intersection_srv_util->idle_time);

        for(unsigned int k = 0; k < response_data[j].posting_list->size(); k++)
        {
            union_reply->add_doc_ids(response_data[j].posting_list->at(k));
        }
    }
    create_intersection_srv_req_time = create_intersection_srv_req_time/number_of_intersection_servers;
    unpack_intersection_srv_resp_time = unpack_intersection_srv_resp_time/number_of_intersection_servers;
    unpack_intersection_srv_req_time = unpack_intersection_srv_req_time/number_of_intersection_servers;
    calculate_intersection_time = calculate_intersection_time/number_of_intersection_servers;
    pack_intersection_srv_resp_time = pack_intersection_srv_resp_time/number_of_intersection_servers;

    union_reply->set_create_intersection_srv_req_time(create_intersection_srv_req_time);
    union_reply->set_unpack_intersection_srv_resp_time(unpack_intersection_srv_resp_time);
    union_reply->set_unpack_intersection_srv_req_time(unpack_intersection_srv_req_time);
    union_reply->set_calculate_intersection_time(calculate_intersection_time);
    union_reply->set_pack_intersection_srv_resp_time(pack_intersection_srv_resp_time);
    union_reply->set_number_of_intersection_servers(number_of_intersection_servers);
}

void PackUnionServiceResponse(const std::vector<Docids> &final_posting_list,
        const struct ThreadArgs* thread_args,
        const unsigned int number_of_intersection_servers,
        union_service::UnionResponse* union_reply)
{
    unsigned int posting_list_size = final_posting_list.size();
    for(unsigned int i = 0; i < posting_list_size; i++)
    {
        union_reply->add_doc_ids(final_posting_list[i]);
    }
    for(unsigned int i = 0; i < number_of_intersection_servers; i++)
    {
        union_service::Util* intersection_srv_util = union_reply->mutable_util_response()->add_intersection_srv_util();
        intersection_srv_util->set_user_time(thread_args[i].intersection_srv_util.user_time);
        intersection_srv_util->set_system_time(thread_args[i].intersection_srv_util.system_time);
        intersection_srv_util->set_io_time(thread_args[i].intersection_srv_util.io_time);
        intersection_srv_util->set_idle_time(thread_args[i].intersection_srv_util.idle_time);
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
            case 1:
                {
                    FMSyncTMNames tm_name = sdb20_50;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(20, 50, 0)));
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
                    FMAsyncTMNames tm_name = adb4_4_4;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMAsyncTMNames, TMConfig>(tm_name, TMConfig(4, 4, 4)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 2:
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

