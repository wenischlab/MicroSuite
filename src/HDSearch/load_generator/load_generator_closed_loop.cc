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

#include "helper_files/mid_tier_client_helper.h"
#include "helper_files/timing.h"

#define QUERIES_TOTAL 100000

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using loadgen_index::LoadGenRequest;
using loadgen_index::ResponseIndexKnnQueries;
using loadgen_index::LoadGenIndex;

unsigned int number_of_bucket_servers = 0;
int num_requests = 0, util_requests = 0;
float qps = 0;
std::string ip = "localhost";
bool resp_recvd = false;
int responses_recvd = 0;
std::string timing_file_name = "", qps_file_name = "";
std::map<void*, uint64_t> resp_times;
uint64_t interval_start_time = GetTimeInSec();
UtilInfo* previous_util = new UtilInfo();
GlobalStats* global_stats = new GlobalStats();
bool last_request = false, start_counter = false, first_req_flag = false;
std::mutex responses_recvd_mutex, start_counter_mutex, outstanding_mutex, global_stats_mutex;
uint64_t outstanding = 0;

std::map<uint64_t, uint64_t> start_map;
std::vector<uint64_t> end_vec;
std::vector<uint64_t> start_vec;

class LoadGenIndexClient {
    public:
        explicit LoadGenIndexClient(std::shared_ptr<Channel> channel)
            : stub_(LoadGenIndex::NewStub(channel)) {}

        // Assembles the client's payload and sends it to the server.
        void LoadGen_Index(MultiplePoints* queries,
                uint64_t query_id,
                const unsigned &queries_size,
                const unsigned &number_of_nearest_neighbors,
                const bool util_request,
                const bool last_request) {
            uint64_t start = GetTimeInMicro();
            // Get the dimension
            int dimension = queries->GetPointAtIndex(0).GetSize();
            // Declare the set of queries & #NN that must be sent.
            LoadGenRequest load_gen_request;

            // Create RCP request by adding queries and number of NN.
            CreateIndexServiceRequest(*queries,
                    query_id,
                    queries_size,
                    number_of_nearest_neighbors,
                    dimension,
                    util_request,
                    &load_gen_request);
            load_gen_request.set_last_request(last_request);

            // Call object to store rpc data
            AsyncClientCall* call = new AsyncClientCall;
            resp_times[(void*)call] = GetTimeInMicro();
            call->index_reply.set_create_index_req_time(GetTimeInMicro() - start); 

            // stub_->AsyncSayHello() performs the RPC call, returning an instance to
            // store in "call". Because we are using the asynchronous API, we need to
            // hold on to the "call" instance in order to get updates on the ongoing RPC.
            uint64_t request_id = reinterpret_cast<uintptr_t>(this);
            start_map[request_id] = GetTimeInMicro();
            call->response_reader = stub_->AsyncLoadGen_Index(&call->context, load_gen_request, &cq_);

            // Request that, upon completion of the RPC, "reply" be updated with the
            // server's response; "status" with the indication of whether the operation
            // was successful. Tag the request with the memory address of the call object.
            call->response_reader->Finish(&call->index_reply, &call->status, (void*)call);
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
                uint64_t request_id = call->index_reply.request_id();
                start_map[request_id] = start_map[request_id] - call->index_reply.start_stamp();
                responses_recvd_mutex.lock();
                end_vec.emplace_back(GetTimeInMicro() - call->index_reply.end_stamp());
                start_vec.emplace_back(start_map[request_id]);
                responses_recvd_mutex.unlock();

                // Verify that the request was completed successfully. Note that "ok"
                // corresponds solely to the request for updates introduced by Finish().
                /*if (last_request && !ok) {
                  exit(0);
                  }*/
                //GPR_ASSERT(ok);

                if (call->status.ok())
                {
                    outstanding_mutex.lock();
                    outstanding--;
                    outstanding_mutex.unlock();
                    if (start_flag) {
                        if(util_requests == 1 && !first_req_flag) {
                            first_req_flag = true;
                            number_of_bucket_servers = call->index_reply.number_of_bucket_servers();
                            previous_util->bucket_util = new Util[number_of_bucket_servers];
                            global_stats->percent_util_info.bucket_util_percent = new PercentUtil[number_of_bucket_servers];
                        }
                        resp_recvd = true;
                        uint64_t start_time, end_time;
                        DistCalc knn_answer;
                        TimingInfo* timing_info = new TimingInfo();
                        PercentUtilInfo* percent_util_info = new PercentUtilInfo();
                        percent_util_info->bucket_util_percent = new PercentUtil[number_of_bucket_servers];
                        start_time = GetTimeInMicro();
                        UnpackIndexServiceResponse(call->index_reply,
                                &knn_answer,
                                timing_info,
                                previous_util,
                                percent_util_info);
                        end_time = GetTimeInMicro();
                        timing_info->unpack_index_resp_time = end_time - start_time;
                        timing_info->create_index_req_time = call->index_reply.create_index_req_time();
                        timing_info->update_index_util_time = call->index_reply.update_index_util_time();
                        timing_info->get_bucket_responses_time = call->index_reply.get_bucket_responses_time();
                        /*WriteKNNToFile("/home/liush/highdimensionalsearch/results/async_results.txt", 
                          knn_answer);*/
                        resp_times[(void*)call] = GetTimeInMicro() - resp_times[(void*)call];
                        timing_info->total_resp_time = resp_times[(void*)call];
                        // std::cout << timing_info->total_resp_time << " " << timing_info->get_bucket_responses_time << "\n";
                        global_stats_mutex.lock();
                        UpdateGlobalTimingStats(*timing_info,
                                global_stats);
                        global_stats_mutex.unlock();
                        if((util_requests != 1) && (call->index_reply.util_response().util_present())) {
                            UpdateGlobalUtilStats(percent_util_info, 
                                    number_of_bucket_servers,
                                    global_stats);
                        }
                        //WriteTimingInfoToFile(timing_file, *timing_info);
                        responses_recvd_mutex.lock();
                        responses_recvd++;
                        responses_recvd_mutex.unlock();
                    }

                } else {
                    std::cout << " * " << std::endl;
                    CHECK(false, "");
                }

                // Once we're complete, deallocate the call object.
                //delete call;
            }
            //if (r == CompletionQueue::TIMEOUT) continue;
            //}

        }

            private:

        // struct for keeping state and data information
        struct AsyncClientCall {
            // Container for the data we expect from the server.
            ResponseIndexKnnQueries index_reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // Storage for the status of the RPC upon completion.
            Status status;


            std::unique_ptr<ClientAsyncResponseReader<ResponseIndexKnnQueries>> response_reader;
        };

        // Out of the passed in Channel comes the stub, stored here, our view of the
        // server's exposed services.
        std::unique_ptr<LoadGenIndex::Stub> stub_;

        // The producer-consumer queue we use to communicate asynchronously with the
        // gRPC runtime.
        CompletionQueue cq_;
        };

        int main(int argc, char** argv) {
            /* Get path to queries (batch/single) and the number of
               nearest neighbors via the command line.*/
            unsigned int number_of_nearest_neighbors = 0;
            std::string queries_file_name, result_file_name;
            struct LoadGenCommandLineArgs* load_gen_command_line_args = new struct LoadGenCommandLineArgs();
            load_gen_command_line_args = ParseLoadGenCommandLine(argc,
                    argv);
            queries_file_name = load_gen_command_line_args->queries_file_name;
            result_file_name = load_gen_command_line_args->result_file_name;
            number_of_nearest_neighbors = load_gen_command_line_args->number_of_nearest_neighbors;
            uint64_t time_duration = load_gen_command_line_args->time_duration;
            qps = load_gen_command_line_args->qps;
            ip = load_gen_command_line_args->ip;
            qps_file_name = load_gen_command_line_args->qps_file_name;
            timing_file_name = load_gen_command_line_args->timing_file_name;
            std::string util_file_name = load_gen_command_line_args->util_file_name;
            CHECK((time_duration >= 0), "ERROR: Offered load (time in seconds) must always be a positive value");
            struct TimingInfo timing_info;
            // Create queries from query file.
            MultiplePoints queries;
            uint64_t start_time, end_time;
            start_time = GetTimeInMicro();
            CreatePointsFromBinFile(queries_file_name, &queries);
            end_time = GetTimeInMicro();
            timing_info.create_queries_time = end_time - start_time;

            // Instantiate the client. It requires a channel, out of which the actual RPCs
            // are created. This channel models a connection to an endpoint (in this case,
            // localhost at port 50051). We indicate that the channel isn't authenticated
            // (use of InsecureChannelCredentials()).
            std::string ip_port = ip;
            LoadGenIndexClient loadgen_index(grpc::CreateChannel(
                        ip_port, grpc::InsecureChannelCredentials()));

            // Spawn reader thread that loops indefinitely
            
            std::thread thread_ = std::thread(&LoadGenIndexClient::AsyncCompleteRpc, &loadgen_index);
            uint64_t query_id = rand() % QUERIES_TOTAL;
            MultiplePoints query(1, queries.GetPointAtIndex(query_id));

            //To calculate max throughput of the system.
            uint64_t requests_sent = 0;
            double curr_time = (double)GetTimeInMicro();
            double warm_up = curr_time + double(20*1000000);
            double exit_time = warm_up + double((time_duration)*1000000);
            bool flag = false;

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
                    loadgen_index.LoadGen_Index(&query,
                            query_id,
                            query.GetSize(),
                            number_of_nearest_neighbors,
                            false,
                            last_request);
                    requests_sent++;
                    query_id = rand() % QUERIES_TOTAL;
                    query.SetPoint(0, queries.GetPointAtIndex(query_id));
                }
                curr_time = (double)GetTimeInMicro();
            }
            start_counter_mutex.lock();
            start_counter = true;
            start_counter_mutex.unlock();
            flag = false;
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
                        loadgen_index.LoadGen_Index(&query,
                                query_id,
                                query.GetSize(),
                                number_of_nearest_neighbors,
                                true,
                                last_request);
                        interval_start_time = GetTimeInSec();
                    } else {
                        loadgen_index.LoadGen_Index(&query,
                                query_id,
                                query.GetSize(),        
                                number_of_nearest_neighbors,                    
                                false,                                                                  
                                last_request);                                                                                  
                    }
                    requests_sent++;
                    query_id = rand() % QUERIES_TOTAL;
                    query.SetPoint(0, queries.GetPointAtIndex(query_id));
                }
                curr_time = (double)GetTimeInMicro();
            }

            responses_recvd_mutex.lock();
            std::cout << std::endl;
            std::cout << responses_recvd << std::endl;
            std::cout << responses_recvd/time_duration << std::endl;
            std::cout.flush();
            //PrintTimingHistogram(start_vec);
            //PrintTimingHistogram(end_vec);
            responses_recvd_mutex.unlock();

            global_stats_mutex.lock();
            PrintGlobalStats(*global_stats,
                    number_of_bucket_servers,
                    (util_requests-1),
                    responses_recvd);
            std::cout.flush();
            std::cout << std::endl;
            PrintUtil(*global_stats,
                    number_of_bucket_servers,
                    (util_requests-1));
            std::cout << std::endl;
                global_stats_mutex.unlock();
                std::string s = "./kill_index_server_empty " + ip;
                char* cmd = new char[s.length() + 1];
                std::strcpy(cmd, s.c_str());
                ExecuteShellCommand(cmd);
                exit(0);
                /*WriteToUtilFile(util_file_name, 
                 *global_stats,
                 number_of_bucket_servers,
                 (util_requests-1));*/
                thread_.join();  //blocks forever

                return 0;
            }
