/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <iostream>
#include <memory>
#include <random>
#include <stdlib.h> 
#include <string>
#include <sys/time.h>
#include <unistd.h>

#include <grpc++/grpc++.h>
#include <thread>

#include "mid_tier_service/src/atomics.cpp"
#include "load_generator/helper_files/loadgen_router_client_helper.h"
#include "mid_tier_service/service/helper_files/timing.h"
#include "mid_tier_service/service/helper_files/utils.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using router::RouterRequest;
using router::UtilRequest;
using router::UtilResponse;
using router::LookupResponse;
using router::RouterService;

unsigned int number_of_lookup_servers = 0;
Atomics* num_requests = new Atomics();
Atomics* responses_recvd = new Atomics();
Atomics* util_requests = new Atomics();

float qps = 0.0;
std::string ip = "localhost";
bool resp_recvd = false, kill_ack = false;
std::mutex resp_recvd_mutex, last_req_mutex;
std::mutex responses_recvd_mutex, start_counter_mutex, outstanding_mutex, global_stats_mutex, kill_ack_lock;
std::string timing_file_name = "", qps_file_name = "";
uint64_t interval_start_time = GetTimeInSec();
UtilInfo* previous_util = new UtilInfo();
GlobalStats* global_stats = new GlobalStats();
bool last_request = false, start_counter = false, first_req_flag = false;
uint64_t outstanding = 0;
std::vector<uint64_t> router_times, lookup_times;
std::map<uint64_t, uint64_t> resp_times;

int num_inline = 0, num_workers = 0, num_resp = 0, get_ratio = 0, set_ratio = 0, get_cnt = 0, set_cnt = 0;

std::map<uint64_t, uint64_t> start_map;
std::vector<uint64_t> end_vec;
std::vector<uint64_t> start_vec;

class RouterServiceClient {
    public:
        explicit RouterServiceClient(std::shared_ptr<Channel> channel)
            : stub_(RouterService::NewStub(channel)) {}

        // Assembles the client's payload and sends it to the server.
        void Router(const std::string key,
                const std::string value,
                const int operation,
                const bool util_request,
                const bool kill) 
        {
            RouterRequest router_request;
            uint64_t start = GetTimeInMicro();
            CreateRouterServiceRequest(key,
                    value,
                    operation,
                    util_request,
                    &router_request);
            router_request.set_kill(kill);

            router_request.set_last_request(last_request);
            router_request.set_resp_time(GetTimeInMicro());

            // Call object to store rpc data
            AsyncClientCall* call = new AsyncClientCall;
            uint64_t request_id = num_requests->AtomicallyReadCount();
            router_request.set_request_id(request_id);
            resp_times[request_id] = GetTimeInMicro();

            call->router_reply.set_create_router_req_time(GetTimeInMicro() - start);

            // stub_->AsyncSayHello() performs the RPC call, returning an instance to
            // store in "call". Because we are using the asynchronous API, we need to
            // hold on to the "call" instance in order to get updates on the ongoing RPC.

            try {
                call->response_reader = stub_->AsyncRouter(&call->context, router_request, &cq_);

                // Request that, upon completion of the RPC, "reply" be updated with the
                // server's response; "status" with the indication of whether the operation
                // was successful. Tag the request with the memory address of the call object.
                call->response_reader->Finish(&call->router_reply, &call->status, (void*)call);
            } catch( ... ) {
                CHECK(false, " * ");
            }
        }

        // Loop while listening for completed responses.
        // Prints out the response from the server.
        void AsyncCompleteRpc() {
            void* got_tag;
            bool ok = false;
            //std::string timing_file_name_final = timing_file_name + std::to_string(qps) + ".txt";
            //std::ofstream timing_file;
            // Block until the next result is available in the completion queue "cq".
            //while(true)
            //{
            //auto r = cq_.AsyncNext(&got_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
            //if (r == CompletionQueue::GOT_EVENT) {
            while (cq_.Next(&got_tag, &ok)) {

                //timing_file.open(timing_file_name_final, std::ios_base::app);
                // The tag in this example is the memory location of the call object
                AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
                // Verify that the request was completed successfully. Note that "ok"
                // corresponds solely to the request for updates introduced by Finish().
                //GPR_ASSERT(ok);

                if (call->status.ok())
                {
                    kill_ack_lock.lock();
                    if (call->router_reply.kill_ack()) {
                        kill_ack = true;
                        std::cout << "got kill ack\n";
                        std::cout << std::flush;
                    }
                    kill_ack_lock.unlock();

                    if(util_requests->AtomicallyReadCount() == 1 && !first_req_flag) {
                        first_req_flag = true;
                        number_of_lookup_servers = call->router_reply.number_of_lookup_servers();
                        previous_util->lookup_srv_util = new Util[number_of_lookup_servers];
                        global_stats->percent_util_info.lookup_srv_util_percent = new PercentUtil[number_of_lookup_servers];
                    }

                    uint64_t request_id = call->router_reply.request_id();
                    resp_times[request_id] = GetTimeInMicro() - resp_times[request_id];
                    resp_recvd = true;
                    uint64_t start_time, end_time;
                    std::string value;
                    TimingInfo* timing_info = new TimingInfo();
                    PercentUtilInfo* percent_util_info = new PercentUtilInfo();
                    percent_util_info->lookup_srv_util_percent = new PercentUtil[number_of_lookup_servers];
                    start_time = GetTimeInMicro();
                    UnpackRouterServiceResponse(call->router_reply,
                            &value,
                            timing_info,
                            previous_util,
                            percent_util_info);
                    end_time = GetTimeInMicro();

                    timing_info->unpack_router_resp_time = end_time - start_time;
                    timing_info->create_router_req_time = call->router_reply.create_router_req_time();
                    timing_info->update_router_util_time = call->router_reply.update_router_util_time();
                    timing_info->get_lookup_srv_responses_time = call->router_reply.get_lookup_srv_responses_time();
                    timing_info->total_resp_time = resp_times[request_id];

                    resp_times[request_id] = 0;


                    global_stats_mutex.lock();
                    UpdateGlobalTimingStats(*timing_info,
                            global_stats);
                    global_stats_mutex.unlock();

                    if((util_requests->AtomicallyReadCount() != 1) && (call->router_reply.util_response().util_present())) {
                        UpdateGlobalUtilStats(percent_util_info,
                                number_of_lookup_servers,
                                global_stats);
                    }

                    responses_recvd->AtomicallyIncrementCount();
                    if (responses_recvd->AtomicallyReadCount() == 1) { 
                        // Print the index config that we got this data for.
                        std::cout << call->router_reply.num_inline() << " " << call->router_reply.num_workers() << " " << call->router_reply.num_resp() << " ";

                    }

                } else {
                    sleep(2);
                    std::string s = "./kill_router_server_empty " + ip;
                    char* cmd = new char[s.length() + 1];
                    std::strcpy(cmd, s.c_str());
                    ExecuteShellCommand(cmd);
                    std::cout << "Load generator failed\n";
                    CHECK(false, "");
                }

                delete call;
            } 
        }

            private:

        // struct for keeping state and data information
        struct AsyncClientCall {
            // Container for the data we expect from the server.
            LookupResponse router_reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // Storage for the status of the RPC upon completion.
            Status status;


            std::unique_ptr<ClientAsyncResponseReader<LookupResponse>> response_reader;
        };

        // Out of the passed in Channel comes the stub, stored here, our view of the
        // server's exposed services.
        std::unique_ptr<RouterService::Stub> stub_;

        // The producer-consumer queue we use to communicate asynchronously with the
        // gRPC runtime.
        CompletionQueue cq_;
        };

        int main(int argc, char** argv) {
            std::string queries_file_name, result_file_name;
            struct LoadGenCommandLineArgs* load_gen_command_line_args = new struct LoadGenCommandLineArgs();
            load_gen_command_line_args = ParseLoadGenCommandLine(argc,
                    argv);
            queries_file_name = load_gen_command_line_args->queries_file_name;
            result_file_name = load_gen_command_line_args->result_file_name;
            uint64_t time_duration = load_gen_command_line_args->time_duration;
            qps = load_gen_command_line_args->qps;
            ip = load_gen_command_line_args->ip;
            get_ratio = load_gen_command_line_args->get_ratio;
            set_ratio = load_gen_command_line_args->set_ratio;
            CHECK((time_duration >= 0), "ERROR: Offered load (time in seconds) must always be a positive value");
            struct TimingInfo timing_info;
            uint64_t start_time, end_time;
            start_time = GetTimeInMicro();

            std::vector<std::pair<std::string, std::string> > queries;
            CreateQueriesFromBinFile(queries_file_name,
                    &queries);
            end_time = GetTimeInMicro();
            timing_info.create_queries_time = end_time - start_time;

            std::string ip_port = ip;
            RouterServiceClient router_client(grpc::CreateChannel(
                        ip_port, grpc::InsecureChannelCredentials()));

            // Spawn reader thread that loops indefinitely
            std::thread thread_ = std::thread(&RouterServiceClient::AsyncCompleteRpc, &router_client);
            uint64_t query_id = rand() % queries.size();
            std::string key = std::get<0>(queries[query_id]);
            std::string value = std::get<1>(queries[query_id]);
            int operation = 1;
            if (get_ratio >= set_ratio) {
                get_cnt = (get_cnt + 1) % (get_ratio + 1);
            }
            if (set_ratio > get_ratio) {
                operation = 2;
                set_cnt = (set_cnt + 1) % (set_ratio + 1);
            }

            double center = 1000000.0/(double)(qps);
            double curr_time = (double)GetTimeInMicro();
            double exit_time = curr_time + (double)(time_duration*1000000.0);

            std::default_random_engine generator;
            std::poisson_distribution<int> distribution(center);
            double next_time = distribution(generator) + curr_time;

            while (curr_time < exit_time) 
            {
                if (curr_time >= next_time) {
                    num_requests->AtomicallyIncrementCount();
                    if((num_requests->AtomicallyReadCount() == 1) || ((GetTimeInSec() - interval_start_time) >= 10)){
                        util_requests->AtomicallyIncrementCount();
                        router_client.Router(key,
                                value,
                                operation,
                                true,
                                false);
                        interval_start_time = GetTimeInSec();
                    } else {
                        router_client.Router(key,
                                value,                       
                                operation,                                               
                                false,
                                false);  
                    }
                    next_time = distribution(generator) + curr_time;
                    query_id = rand() % queries.size();
                    key = std::get<0>(queries[query_id]);
                    value = std::get<1>(queries[query_id]);
                    if (get_ratio >= set_ratio) {
                        if (get_cnt == get_ratio) {
                            if (set_cnt < set_ratio) {
                                operation = 2;
                                set_cnt = (set_cnt + 1) % (set_ratio + 1);
                            } else {
                                operation = 1;
                                get_cnt = (get_cnt + 1) % (get_ratio + 1);
                            }
                        } else if (get_cnt < get_ratio) {
                            operation = 1;
                            get_cnt = (get_cnt + 1) % (get_ratio + 1);
                        } else {
                            CHECK(false, "Get count cannot exceed get ratio\n");
                        }
                    } else {
                        if (set_cnt == set_ratio) {
                            if (get_cnt < get_ratio) {
                                operation = 1;
                                get_cnt = (get_cnt + 1) % (get_ratio + 1);
                            } else {
                                operation = 2;
                                set_cnt = (set_cnt + 1) % (set_ratio + 1);
                            }
                        } else if (set_cnt < set_ratio) {
                            operation = 2;
                            set_cnt = (set_cnt + 1) % (set_ratio + 1);
                        } else {
                            CHECK(false, "Set count cannot exceed set ratio\n");
                        }
                    }

                }
                curr_time = (double)GetTimeInMicro();
            }

            float achieved_qps = (float)responses_recvd->AtomicallyReadCount()/(float)time_duration;

            global_stats_mutex.lock();
            PrintLatency(*global_stats,
                    number_of_lookup_servers,
                    (util_requests->AtomicallyReadCount() - 1),
                    responses_recvd->AtomicallyReadCount());

            float query_cost = ComputeQueryCost(*global_stats, 
                    (util_requests->AtomicallyReadCount() - 1),
                    number_of_lookup_servers, 
                    achieved_qps);
            global_stats_mutex.unlock();

            std::cout << " " << query_cost << " ";

            while (true) {
                //std::cout << "trying to send kill\n";
                std::cout << std::flush;
                kill_ack_lock.lock();
                if (kill_ack) {
                    //std::cout << "got kill ack dying\n";
                    //std::cout << std::flush;
                    CHECK(false, "");
                }
                kill_ack_lock.unlock();
                sleep(2);

                router_client.Router(key,
                        value,
                        operation,
                        false,
                        true);
            }

            CHECK(false, "Load generator exiting\n");

            thread_.join(); 
            return 0;

        }
