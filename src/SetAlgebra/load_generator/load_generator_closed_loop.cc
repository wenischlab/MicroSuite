/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <iostream>
#include <memory>
#include <random>
#include <stdlib.h> 
#include <string>
#include <sys/time.h>

#include <grpc++/grpc++.h>
#include <thread>

#include "load_generator/helper_files/loadgen_union_client_helper.h"
#include "union_service/service/helper_files/timing.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using union_service::UnionRequest;
using union_service::UtilRequest;
using union_service::UtilResponse;
using union_service::UnionResponse;
using union_service::UnionService;

unsigned int number_of_intersection_servers = 0;
long num_requests = 0, util_requests = 0;
float qps = 0.0;
std::string ip = "localhost";
bool resp_recvd = false;
std::mutex resp_recvd_mutex, last_req_mutex;
long responses_recvd = 0;
std::mutex responses_recvd_mutex, start_counter_mutex, outstanding_mutex, global_stats_mutex;;
std::string timing_file_name = "", qps_file_name = "";
uint64_t interval_start_time = GetTimeInSec();
UtilInfo* previous_util = new UtilInfo();
GlobalStats* global_stats = new GlobalStats();
bool last_request = false, start_counter = false, first_req_flag = false;
uint64_t outstanding = 0;
std::vector<uint64_t> union_times, intersection_times;
std::map<void*, uint64_t> resp_times;

std::map<uint64_t, uint64_t> start_map;
std::vector<uint64_t> end_vec;
std::vector<uint64_t> start_vec;

class UnionServiceClient {
    public:
        explicit UnionServiceClient(std::shared_ptr<Channel> channel)
            : stub_(UnionService::NewStub(channel)) {}

        // Assembles the client's payload and sends it to the server.
        void Union(const std::vector<Wordids> word_ids,
                const bool util_request,
                const bool last_request) 
        {
            UnionRequest union_request;
            uint64_t start = GetTimeInMicro();
            CreateUnionServiceRequest(word_ids,
                    util_request,
                    &union_request);

            union_request.set_last_request(last_request);
            union_request.set_resp_time(GetTimeInMicro());

            // Call object to store rpc data
            AsyncClientCall* call = new AsyncClientCall;
            call->union_reply.set_create_union_req_time(GetTimeInMicro() - start);
            resp_times[(void*)call] = GetTimeInMicro();

            // stub_->AsyncSayHello() performs the RPC call, returning an instance to
            // store in "call". Because we are using the asynchronous API, we need to
            // hold on to the "call" instance in order to get updates on the ongoing RPC.
            uint64_t request_id = reinterpret_cast<uintptr_t>(this);
            start_map[request_id] = GetTimeInMicro();

            try {
                call->response_reader = stub_->AsyncUnion(&call->context, union_request, &cq_);

                // Request that, upon completion of the RPC, "reply" be updated with the
                // server's response; "status" with the indication of whether the operation
                // was successful. Tag the request with the memory address of the call object.
                call->response_reader->Finish(&call->union_reply, &call->status, (void*)call);
            } catch( ... ) {
                CHECK(false, " * ");
            }
        }

        // Loop while listening for completed responses.
        // Prints out the response from the server.
        void AsyncCompleteRpc() {
            void* got_tag;
            bool ok = false;
            bool start_flag = false;
            //std::string timing_file_name_final = timing_file_name + std::to_string(qps) + ".txt";
            //std::ofstream timing_file;
            // Block until the next result is available in the completion queue "cq".
            //while(true)
            //{
            //auto r = cq_.AsyncNext(&got_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
            //if (r == CompletionQueue::GOT_EVENT) {
            while (cq_.Next(&got_tag, &ok)) {
                if (!start_flag) {
                    start_counter_mutex.lock();
                    if (start_counter) {
                        start_flag = true;
                    }
                    start_counter_mutex.unlock();
                }

                //timing_file.open(timing_file_name_final, std::ios_base::app);
                // The tag in this example is the memory location of the call object
                AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
                uint64_t request_id = call->union_reply.request_id();
                start_map[request_id] = start_map[request_id] - call->union_reply.start_stamp();
                responses_recvd_mutex.lock();
                end_vec.emplace_back(GetTimeInMicro() - call->union_reply.end_stamp());
                start_vec.emplace_back(start_map[request_id]);
                responses_recvd_mutex.unlock();
                // Verify that the request was completed successfully. Note that "ok"
                // corresponds solely to the request for updates introduced by Finish().
                /*last_req_mutex.lock();
                  if (last_request && !ok) {
                  exit(0);
                  }
                  last_req_mutex.unlock();*/
                //GPR_ASSERT(ok);

                if (call->status.ok())
                {
                    outstanding_mutex.lock();
                    outstanding--;
                    outstanding_mutex.unlock();
                    if (start_flag) {
                        if(util_requests == 1 && !first_req_flag) {
                            first_req_flag = true;
                            number_of_intersection_servers = call->union_reply.number_of_intersection_servers();
                            previous_util->intersection_srv_util = new Util[number_of_intersection_servers];
                            global_stats->percent_util_info.intersection_srv_util_percent = new PercentUtil[number_of_intersection_servers];
                        }
                        resp_recvd = true;
                        uint64_t start_time, end_time;
                        std::vector<Docids> posting_list;
                        TimingInfo* timing_info = new TimingInfo();
                        PercentUtilInfo* percent_util_info = new PercentUtilInfo();
                        percent_util_info->intersection_srv_util_percent = new PercentUtil[number_of_intersection_servers];
                        start_time = GetTimeInMicro();
                        UnpackUnionServiceResponse(call->union_reply,
                                &posting_list,
                                timing_info,
                                previous_util,
                                percent_util_info);
                        end_time = GetTimeInMicro();

                        timing_info->unpack_union_resp_time = end_time - start_time;
                        timing_info->create_union_req_time = call->union_reply.create_union_req_time();
                        timing_info->update_union_util_time = call->union_reply.update_union_util_time();
                        timing_info->get_intersection_srv_responses_time = call->union_reply.get_intersection_srv_responses_time();

                        resp_times[(void*)call] = GetTimeInMicro() - resp_times[(void*)call];
                        timing_info->total_resp_time = resp_times[(void*)call];

                        global_stats_mutex.lock();
                        UpdateGlobalTimingStats(*timing_info,
                                global_stats);
                        global_stats_mutex.unlock();

                        if((util_requests != 1) && (call->union_reply.util_response().util_present())) {
                            UpdateGlobalUtilStats(percent_util_info, 
                                    number_of_intersection_servers,
                                    global_stats);
                        }
                        //WriteTimingInfoToFile(timing_file, *timing_info);
                        responses_recvd_mutex.lock();
                        responses_recvd++;
                        responses_recvd_mutex.unlock();
                    }

                } else {
                    std::cout << " * ";
                    CHECK(false, "");
                    //std::cout << "RPC failed" << std::endl;
                }

                // Once we're complete, deallocate the call object.
                delete call;
            }
            //if (r == CompletionQueue::TIMEOUT) continue;
            //}

        }

            private:

        // struct for keeping state and data information
        struct AsyncClientCall {
            // Container for the data we expect from the server.
            UnionResponse union_reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // Storage for the status of the RPC upon completion.
            Status status;


            std::unique_ptr<ClientAsyncResponseReader<UnionResponse>> response_reader;
        };

        // Out of the passed in Channel comes the stub, stored here, our view of the
        // server's exposed services.
        std::unique_ptr<UnionService::Stub> stub_;

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
            CHECK((time_duration >= 0), "ERROR: Offered load (time in seconds) must always be a positive value");
            struct TimingInfo timing_info;
            uint64_t start_time, end_time;
            start_time = GetTimeInMicro();

            std::vector<std::vector<Wordids> > queries;
            CreateQueriesFromFile(queries_file_name,
                    &queries);
            long queries_size = queries.size();
            end_time = GetTimeInMicro();
            timing_info.create_queries_time = end_time - start_time;

            std::string ip_port = ip;
            UnionServiceClient union_client(grpc::CreateChannel(
                        ip_port, grpc::InsecureChannelCredentials()));

            // Spawn reader thread that loops indefinitely
            std::thread thread_ = std::thread(&UnionServiceClient::AsyncCompleteRpc, &union_client);
            long query_id = rand() % queries_size;
            std::vector<Wordids> query = queries[query_id];

            //To calculate max throughput of the system.
            uint64_t requests_sent = 0;
            double curr_time = (double)GetTimeInMicro();
            double warm_up = curr_time + double(20*1000000.0);
            double exit_time = warm_up + double((time_duration)*1000000.0);
            bool flag = false;
            int itr = 0;
            while(curr_time < warm_up) {
                outstanding_mutex.lock();
                if (outstanding < qps) {
                    outstanding++;
                    flag = true;
                } else {
                    flag = false;
                }
                outstanding_mutex.unlock();
                if (flag) {
                    union_client.Union(query,
                            false,
                            last_request);

                    requests_sent++;
                    query_id = rand() % queries_size;
                    query = queries[query_id];
                    itr++;
                }
                curr_time = (double)GetTimeInMicro();
            }
            start_counter_mutex.lock();
            start_counter = true;
            start_counter_mutex.unlock();
            flag = false;
            itr = 0;
            while(curr_time < exit_time) {
                outstanding_mutex.lock();
                if (outstanding < qps) {
                    outstanding++;
                    flag = true;
                } else {
                    flag = false;
                }
                outstanding_mutex.unlock();
                if (flag) {
                    if((requests_sent == 0) || ((GetTimeInSec() - interval_start_time) >= 10)){
                        util_requests++;
                        union_client.Union(query,
                                true,
                                last_request);
                        interval_start_time = GetTimeInSec();
                    } else {
                        union_client.Union(query,
                                false,
                                last_request);

                    }
                    requests_sent++;
                    query_id = rand() % queries_size;
                    query = queries[query_id];
                }
                curr_time = (double)GetTimeInMicro();
            }

            responses_recvd_mutex.lock();
            std::cout << std::endl;
            std::cout << responses_recvd << std::endl;
            std::cout << responses_recvd/time_duration << std::endl;
            std::cout.flush();
            responses_recvd_mutex.unlock();

            global_stats_mutex.lock();
            PrintGlobalStats(*global_stats,
                    number_of_intersection_servers,
                    (util_requests-1),
                    responses_recvd);
            std::cout.flush();
            std::cout << std::endl;
            PrintUtil(*global_stats,
                    number_of_intersection_servers,
                    (util_requests-1));
            std::cout << std::endl;
            global_stats_mutex.unlock();

            thread_.join();  //blocks forever

            return 0;
        }
