#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <unordered_map>
#include "router_server_helper.h"

void GetLookupServerIPs(const std::string &lookup_server_ips_file,
        std::vector<std::string>* lookup_server_ips)
{
    std::ifstream file(lookup_server_ips_file);
    CHECK((file.good()), "ERROR: File containing lookup server IPs must exists\n");
    std::string line = "";
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buffer(line);
        std::istream_iterator<std::string> begin(buffer), end;
        std::vector<std::string> tokens(begin, end);
        /* Tokens must contain only one IP -
           Each IP address must be on a different line.*/
        CHECK((tokens.size() == 1), "ERROR: File must contain only one IP address per line\n");
        lookup_server_ips->push_back(tokens[0]);
    }
}

void UnpackRouterServiceRequest(const router::RouterRequest &router_request,
        std::string* key,
        std::string* value,
        uint32_t* operation,
        lookup::Key* request_to_lookup_srv)
{
    *key = router_request.key();
    *value = router_request.value();
    *operation = router_request.operation();
    request_to_lookup_srv->set_key(*key);
    request_to_lookup_srv->set_value(*value);
    request_to_lookup_srv->set_operation(*operation);    
}

void Merge(struct ThreadArgs* thread_args,
        unsigned int replication_cnt,
        std::string* lookup_val,
        uint64_t* create_lookup_srv_req_time,
        uint64_t* unpack_lookup_srv_resp_time,
        uint64_t* unpack_lookup_srv_req_time,
        uint64_t* lookup_srv_time,
        uint64_t* pack_lookup_srv_resp_time,
        router::LookupResponse* router_reply)
{
    bool nack = false;
    for(unsigned int j = 0; j < replication_cnt; j++) {
        if (thread_args[j].value == "nack") {
            *lookup_val = "nack";
            nack = true;
            break;
        }
        (*create_lookup_srv_req_time) += thread_args[j].lookup_srv_timing_info.create_lookup_srv_request_time;
        (*unpack_lookup_srv_resp_time) += thread_args[j].lookup_srv_timing_info.unpack_lookup_srv_resp_time;
        (*unpack_lookup_srv_req_time) += thread_args[j].lookup_srv_timing_info.unpack_lookup_srv_req_time;
        (*lookup_srv_time) += thread_args[j].lookup_srv_timing_info.lookup_srv_time;
        (*pack_lookup_srv_resp_time) += thread_args[j].lookup_srv_timing_info.pack_lookup_srv_resp_time;
    }
    if (!nack) {
        *lookup_val = "ack";
    }
    router_reply->set_value(*lookup_val);

    (*create_lookup_srv_req_time) = (*create_lookup_srv_req_time)/replication_cnt;
    (*unpack_lookup_srv_resp_time) = (*unpack_lookup_srv_resp_time)/replication_cnt;
    (*unpack_lookup_srv_req_time) = (*unpack_lookup_srv_req_time)/replication_cnt;
    (*lookup_srv_time) = (*lookup_srv_time)/replication_cnt;
    (*pack_lookup_srv_resp_time) = (*pack_lookup_srv_resp_time)/replication_cnt;

    router_reply->set_create_lookup_srv_req_time(*create_lookup_srv_req_time);
    router_reply->set_unpack_lookup_srv_resp_time(*unpack_lookup_srv_resp_time);
    router_reply->set_unpack_lookup_srv_req_time(*unpack_lookup_srv_req_time);
    router_reply->set_lookup_srv_time(*lookup_srv_time);
    router_reply->set_pack_lookup_srv_resp_time(*pack_lookup_srv_resp_time);
    router_reply->set_number_of_lookup_servers(replication_cnt);
}

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const int replication_cnt,
        router::LookupResponse* router_reply)
{
    uint64_t create_lookup_srv_req_time = 0, unpack_lookup_srv_resp_time = 0, unpack_lookup_srv_req_time = 0, lookup_srv_time = 0, pack_lookup_srv_resp_time = 0;
    bool nack = false;
    std::string lookup_val = "";

    for(int i = 0; i < replication_cnt; i++) {
        if (*(response_data[i].value) == "nack") {
            lookup_val = "nack";
            nack = true;
            break;
        }
        create_lookup_srv_req_time += response_data[i].lookup_srv_timing_info->create_lookup_srv_request_time;
        unpack_lookup_srv_resp_time += response_data[i].lookup_srv_timing_info->unpack_lookup_srv_resp_time;
        unpack_lookup_srv_req_time += response_data[i].lookup_srv_timing_info->unpack_lookup_srv_req_time;
        lookup_srv_time += response_data[i].lookup_srv_timing_info->lookup_srv_time;
        pack_lookup_srv_resp_time += response_data[i].lookup_srv_timing_info->pack_lookup_srv_resp_time;
    }
    if (!nack) {
        lookup_val = "ack";
    }
    router_reply->set_value(lookup_val);

    create_lookup_srv_req_time = create_lookup_srv_req_time/replication_cnt;
    unpack_lookup_srv_resp_time = unpack_lookup_srv_resp_time/replication_cnt;
    unpack_lookup_srv_req_time = unpack_lookup_srv_req_time/replication_cnt;
    lookup_srv_time = lookup_srv_time/replication_cnt;
    pack_lookup_srv_resp_time = pack_lookup_srv_resp_time/replication_cnt;

    router_reply->set_create_lookup_srv_req_time(create_lookup_srv_req_time);
    router_reply->set_unpack_lookup_srv_resp_time(unpack_lookup_srv_resp_time);
    router_reply->set_unpack_lookup_srv_req_time(unpack_lookup_srv_req_time);
    router_reply->set_lookup_srv_time(lookup_srv_time);
    router_reply->set_pack_lookup_srv_resp_time(pack_lookup_srv_resp_time);
    router_reply->set_number_of_lookup_servers(replication_cnt);

    /* Pack util info for all bucket servers. We do not
       want the mean because we want to know how each bucket behaves i.e
       does one perform worse than the others / are they uniform? */
    for(int i = 0; i < replication_cnt; i++)
    {
        router::Util* lookup_srv_util = router_reply->mutable_util_response()->add_lookup_srv_util();
        lookup_srv_util->set_user_time(response_data[i].lookup_srv_util->user_time);
        lookup_srv_util->set_system_time(response_data[i].lookup_srv_util->system_time);
        lookup_srv_util->set_io_time(response_data[i].lookup_srv_util->io_time);
        lookup_srv_util->set_idle_time(response_data[i].lookup_srv_util->idle_time);
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
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 10, 0)));
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
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 60, 0)));
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
                    FMSyncTMNames tm_name = sdb50_20;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(50, 20, 0)));
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
                    all_tms->insert(std::pair<FMSyncTMNames, TMConfig>(tm_name, TMConfig(1, 60, 0)));
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

