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

#include "mid_tier_service/src/atomics.cpp"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using loadgen_index::LoadGenRequest;
using loadgen_index::ResponseIndexKnnQueries;
using loadgen_index::LoadGenIndex;

unsigned int number_of_bucket_servers = 0;
Atomics* num_requests = new Atomics();
Atomics* responses_recvd = new Atomics();
Atomics* util_requests = new Atomics();

float qps = 0.0;
std::string ip = "localhost";
bool resp_recvd = false, kill_ack = false;
std::string timing_file_name = "", qps_file_name = "";
std::map<uint64_t, uint64_t> resp_times;
uint64_t interval_start_time = GetTimeInSec();
UtilInfo* previous_util = new UtilInfo();
GlobalStats* global_stats = new GlobalStats();
std::mutex global_stat_mutex;
bool first_req_flag = false;
std::mutex cout_lock, kill_ack_lock;
int num_inline = 0, num_workers = 0, num_resp = 0;

#define NODEBUG

class LoadGenIndexClient {
    public:
        explicit LoadGenIndexClient(std::shared_ptr<Channel> channel)
            : stub_(LoadGenIndex::NewStub(channel)) {}

        // Assembles the client's payload and sends it to the server.
        void LoadGen_Index(MultiplePoints* queries,
                const uint64_t query_id,
                const unsigned &queries_size,
                const unsigned &number_of_nearest_neighbors,
                const bool util_request,
                const bool kill) {
            uint64_t start = GetTimeInMicro();
            // Get the dimension
            int dimension = queries->GetPointAtIndex(0).GetSize();
            // Declare the set of queries & #NN that must be sent.
            LoadGenRequest load_gen_request;
            load_gen_request.set_kill(kill);

            // Create RCP request by adding queries and number of NN.
            CreateIndexServiceRequest(*queries,
                    query_id,
                    queries_size,
                    number_of_nearest_neighbors,
                    dimension,
                    util_request,
                    &load_gen_request);

            // Call object to store rpc data
            AsyncClientCall* call = new AsyncClientCall;
            //uint64_t request_id = reinterpret_cast<uintptr_t>(call);
            uint64_t request_id = num_requests->AtomicallyReadCount();
            load_gen_request.set_request_id(request_id);
            load_gen_request.set_load((int)(qps));
            resp_times[request_id] = GetTimeInMicro();
            call->index_reply.set_create_index_req_time(GetTimeInMicro() - start); 

            /* A.S The following line work if you want 
               to invoke acuracy script.*/
            //cout_lock.lock();
            //std::cout << request_id << " q " << query_id << std::endl;
            //cout_lock.unlock();

            // stub_->AsyncSayHello() performs the RPC call, returning an instance to
            // store in "call". Because we are using the asynchronous API, we need to
            // hold on to the "call" instance in order to get updates on the ongoing RPC.
#ifndef NODEBUG
            std::cout << "before sending req\n";
#endif
            try {
                call->response_reader = stub_->AsyncLoadGen_Index(&call->context, load_gen_request, &cq_);

                // Request that, upon completion of the RPC, "reply" be updated with the
                // server's response; "status" with the indication of whether the operation
                // was successful. Tag the request with the memory address of the call object.
                call->response_reader->Finish(&call->index_reply, &call->status, (void*)call);
            } catch( ... ) {
                std::cout << "Load generator failed ";
                CHECK(false, "");
            }
#ifndef NODEBUG
            std::cout << "after sending req\n";
#endif
        }

        // Loop while listening for completed responses.
        // Prints out the response from the server.
        void AsyncCompleteRpc() {
            void* got_tag;
            bool ok = false;
            //std::string timing_file_name_final = timing_file_name + std::to_string(qps) + ".txt";
            //std::ofstream timing_file;
            // Block until the next result is available in the completion queue "cq".
            while (cq_.Next(&got_tag, &ok)) {
                //timing_file.open(timing_file_name_final, std::ios_base::app);
                // The tag in this example is the memory location of the call object
                AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);


                // Verify that the request was completed successfully. Note that "ok"
                // corresponds solely to the request for updates introduced by Finish().
                //GPR_ASSERT(ok);

                if (call->status.ok())
                {
#ifndef NODEBUG
                    std::cout << "got resp\n";
#endif
                    kill_ack_lock.lock();
                    if (call->index_reply.kill_ack()) {
                        kill_ack = true;
                        std::cout << "got kill ack\n";
                        std::cout << std::flush;
                    }
                    kill_ack_lock.unlock();

                    if(util_requests->AtomicallyReadCount() == 1 && !first_req_flag) {
                        first_req_flag = true;
                        number_of_bucket_servers = call->index_reply.number_of_bucket_servers();
                        previous_util->bucket_util = new Util[number_of_bucket_servers];
                        global_stats->percent_util_info.bucket_util_percent = new PercentUtil[number_of_bucket_servers];
                    }

                    uint64_t request_id = call->index_reply.request_id();
                    resp_times[request_id] = GetTimeInMicro() - resp_times[request_id];
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
                    timing_info->total_resp_time = resp_times[request_id];

                    resp_times[request_id] = 0;
                    //std::cout << timing_info->total_resp_time << std::endl;
                    // std::cout << timing_info->total_resp_time << " " << timing_info->get_bucket_responses_time << "\n";
                    global_stat_mutex.lock();
                    UpdateGlobalTimingStats(*timing_info,
                            global_stats);
                    global_stat_mutex.unlock();
                    if((util_requests->AtomicallyReadCount() != 1) && (call->index_reply.util_response().util_present())) {
                        UpdateGlobalUtilStats(percent_util_info, 
                                number_of_bucket_servers,
                                global_stats);
                    }
                    //WriteTimingInfoToFile(timing_file, *timing_info);

                    /* A.S The following two lines work if you want 
                       to invoke acuracy script.*/
#if 0
                    cout_lock.lock();
                    std::cout << request_id << " ";
                    PrintKNNForAllQueries(knn_answer);
                    cout_lock.unlock();
#endif

                    responses_recvd->AtomicallyIncrementCount();
                    if (responses_recvd->AtomicallyReadCount() == 1) { 
                        // Print the index config that we got this data for.
                        std::cout << call->index_reply.num_inline() << " " << call->index_reply.num_workers() << " " << call->index_reply.num_resp() << " ";

                    }

                } else {
                    sleep(2);
                    std::string s = "./kill_index_server_empty " + ip;
                    char* cmd = new char[s.length() + 1];
                    std::strcpy(cmd, s.c_str());
                    ExecuteShellCommand(cmd);
                    std::cout << "Load generator failed\n";
                    CHECK(false, "");
                }

                // Once we're complete, deallocate the call object.
                delete call;
            }
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
    long queries_size = queries.GetSize();

    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).
    std::string ip_port = ip;
    LoadGenIndexClient loadgen_index(grpc::CreateChannel(
                ip_port, grpc::InsecureChannelCredentials()));

    // Spawn reader thread that loops indefinitely
    std::thread thread_ = std::thread(&LoadGenIndexClient::AsyncCompleteRpc, &loadgen_index);
    //std::thread thread2 = std::thread(&LoadGenIndexClient::AsyncCompleteRpc, &loadgen_index);
    int index = rand() % queries_size;

    MultiplePoints query(1, queries.GetPointAtIndex(index));
    double center = 1000000.0/(double)(qps);
    double curr_time = (double)GetTimeInMicro();
    double exit_time = curr_time + (double)(time_duration*1000000);

    //Declare the poisson distribution
    std::default_random_engine generator;
    std::poisson_distribution<int> distribution(center);
    double next_time = distribution(generator) + curr_time;

    while (curr_time < exit_time) 
    {
        if (curr_time >= next_time) 
        {
            num_requests->AtomicallyIncrementCount();
            /* If this is the first request, we must gather util info
               so that we can track util periodically after every 10 seconds.*/
            /* Before sending every request, we check to see if
               the time (10 sec) has expired, so that we can decide to
               fill the util_request field to get the util info from
               buckets and index. Otherwise, we do not request
               for util info (false).*/
            if((num_requests->AtomicallyReadCount() == 1) || ((GetTimeInSec() - interval_start_time) >= 5)){
                util_requests->AtomicallyIncrementCount();
                loadgen_index.LoadGen_Index(&query,
                        index,
                        query.GetSize(),
                        number_of_nearest_neighbors,
                        true,
                        false);
                interval_start_time = GetTimeInSec();
            } else {
                loadgen_index.LoadGen_Index(&query,
                        index,
                        query.GetSize(),
                        number_of_nearest_neighbors,
                        false,
                        false);
            }
            next_time = distribution(generator) + curr_time;
            index = rand() % queries_size;

            query.SetPoint(0, queries.GetPointAtIndex(index));
        } 
        curr_time = (double)GetTimeInMicro();
    }
    /*std::string qps_file_name_final = qps_file_name + std::to_string(qps) + ".txt";
      std::ofstream qps_file(qps_file_name_final);
      CHECK(qps_file.good(), "ERROR: Could not open QPS file.\n");
      qps_file << qps_to_file << "\n";
      qps_file.close();*/

    float achieved_qps = (float)responses_recvd->AtomicallyReadCount()/(float)time_duration;

    global_stat_mutex.lock();
#if 0
    PrintGlobalStats(*global_stats,
            number_of_bucket_servers,
            (util_requests->AtomicallyReadCount()-1),
            responses_recvd->AtomicallyReadCount());
#endif
    PrintLatency(*global_stats,
            number_of_bucket_servers,
            (util_requests->AtomicallyReadCount() - 1),
            responses_recvd->AtomicallyReadCount());
    PrintGlobalStats(*global_stats,
            number_of_bucket_servers,
            (util_requests->AtomicallyReadCount() - 1),
            responses_recvd->AtomicallyReadCount());
#if 0
    PrintUtil(*global_stats,
            number_of_bucket_servers,
            (util_requests-1));
    global_stat_mutex.unlock();
#endif
    /*WriteToUtilFile(util_file_name, 
     *global_stats,
     number_of_bucket_servers,
     (util_requests-1));*/
    float query_cost = ComputeQueryCost(*global_stats, 
            (util_requests->AtomicallyReadCount() - 1),
            number_of_bucket_servers, 
            achieved_qps);
    global_stat_mutex.unlock();
    std::cout << " " << query_cost << std::endl;

    //std::cout << "kill start\n";
    //std::cout << std::flush;

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
        loadgen_index.LoadGen_Index(&query,
                index,
                query.GetSize(),
                number_of_nearest_neighbors,
                false,
                true);
        //std::cout << "sent kill\n";
        //std::cout << std::flush;
    }


#if 0
    std::string s = "/home/liush/highdimensionalsearch/load_generator/kill_index_server_empty " + ip;
    char* cmd = new char[s.length() + 1];
    std::strcpy(cmd, s.c_str());
    ExecuteShellCommand(cmd);
    std::cout << "kill sent\n";
    std::cout << std::flush;
#endif

    CHECK(false, "Load generator exiting\n");
    thread_.join();  //blocks forever
    //thread2.join();

    return 0;
}
