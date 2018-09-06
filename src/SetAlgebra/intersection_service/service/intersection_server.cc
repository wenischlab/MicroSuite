/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <iostream>
#include <memory>
#include <omp.h>
#include <string>
#include <sys/time.h>
#include <grpc++/grpc++.h>

#include "intersection_service/src/intersection.h"
#include "intersection_service/service/helper_files/server_helper.h"
#include "intersection_service/service/helper_files/timing.h"
#include "intersection_service/service/helper_files/utils.h"

#define NODEBUG

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using intersection::UtilRequest;
using intersection::IntersectionRequest;
using intersection::TimingDataInMicro;
using intersection::UtilResponse;
using intersection::IntersectionResponse;
using intersection::IntersectionService;

std::string ip_port = "", dataset_file_name = "";
std::mutex intersection_mutex;
unsigned int intersection_srv_parallelism = 1, intersection_srv_no = 0, num_intersection_srvs = 1;

std::map<Docids, std::vector<Docids>> word_to_docids;

void ProcessRequest(IntersectionRequest &request,
        IntersectionResponse* reply)
{

    /* If the index server is asking for util info,
       it means the time period has expired, so 
       the intersection must read /proc/stat to provide user, system, io, and idle times.*/
    //printf("%p b %lu\n", request, GetTimeInMicro());
#ifndef NODEBUG
    std::cout << "before util\n";
#endif
    if(request.util_request().util_request())
    {
        uint64_t user_time = 0, system_time = 0, io_time = 0, idle_time = 0;
        GetCpuTimes(&user_time,
                &system_time,
                &io_time,
                &idle_time);
        reply->mutable_util_response()->set_user_time(user_time);
        reply->mutable_util_response()->set_system_time(system_time);
        reply->mutable_util_response()->set_io_time(io_time);
        reply->mutable_util_response()->set_idle_time(idle_time);
        reply->mutable_util_response()->set_util_present(true);
    }
#ifndef NODEBUG
    std::cout << "after util\n";
#endif

    /* Simply copy request id into the reply - this was just a 
       piggyback message.*/
    reply->set_request_id(request.request_id());

    /* Get the current idle time and total time
       so as to calculate the CPU util when the intersection is done.*/
    size_t idle_time_initial = 0, total_time_initial = 0, idle_time_final = 0, total_time_final = 0;
    //GetCpuTimes(&idle_time_initial, &total_time_initial);
    uint64_t start_time = 0, end_time = 0;

    // Unpack received queries and point IDs
    start_time = GetTimeInMicro();
    std::vector<Docids> word_ids;

    UnpackIntersectionServiceRequest(request,
            &word_ids);

    end_time = GetTimeInMicro();
    reply->mutable_timing_data_in_micro()->set_unpack_intersection_srv_req_time_in_micro((end_time - start_time));
#ifndef NODEBUG
    std::cout << "after unpack\n";
#endif

    /* For each word in the query, we should now extract the
       doc ids.*/
    start_time = GetTimeInMicro();
    std::vector<std::vector<Docids> > doc_ids_for_all_words;
    bool res = ExtractDocids(word_ids,
            word_to_docids,
            &doc_ids_for_all_words);
    if (!res) {
        return;
    }
    Docids num_words_in_query = word_ids.size();

#ifndef NODEBUG
    std::cout << "After extraction\n";
#endif

    CHECK( (num_words_in_query > 0), "Query cannot be an empty list of words\n");

    std::vector<Docids> intersection_res;
    if (doc_ids_for_all_words.size() != 0) {
        intersection_res = doc_ids_for_all_words[0];
    }

    for (int intersection_opr_cnt = 1; intersection_opr_cnt < doc_ids_for_all_words.size(); intersection_opr_cnt++)
    {
        std::vector<Docids> intermediate_res;
        ComputeIntersection(intersection_res,
                doc_ids_for_all_words[intersection_opr_cnt],
                &intermediate_res);
        if (intermediate_res.size() == 0) {
            break;
        }
        intersection_res = intermediate_res;
    }
    reply->mutable_timing_data_in_micro()->set_calculate_intersection_time_in_micro((GetTimeInMicro() - start_time));

#ifndef NODEBUG
    std::cout << "after getting or setting\n";
#endif

    start_time = GetTimeInMicro();
    PackIntersectionServiceResponse(intersection_res,
            reply);
    reply->mutable_timing_data_in_micro()->set_pack_intersection_srv_resp_time_in_micro((GetTimeInMicro() - start_time));

    // Convert K-NN into form suitable for GRPC.
    const float idle_time_delta = idle_time_final - idle_time_initial;
    const float total_time_delta = total_time_final - total_time_initial;
    const float cpu_util = (100.0 * (1.0 - (idle_time_delta/total_time_delta)));
    reply->mutable_timing_data_in_micro()->set_cpu_util(cpu_util);
    reply->set_index_view(request.index_view());

#ifndef NODEBUG
    std::cout << "setting status as ok\n";
#endif
    reply->set_send_stamp(GetTimeInMicro());

}

// Logic and data behind the server's behavior.
class ServiceImpl final {
    public:
        ~ServiceImpl() {
            server_->Shutdown();
            // Always shutdown the completion queue after the server.
            cq_->Shutdown();
        }
        // There is no shutdown handling in this code.
        void Run() {
            std::string server_address(ip_port);
            ServerBuilder builder;
            // Listen on the given address without any authentication mechanism.
            try
            {
                builder.AddListeningPort(server_address,
                        grpc::InsecureServerCredentials());
            } catch(...) {
                CHECK(false, "ERROR: Enter a valid IP address follwed by port number - IP:Port number\n");
            }
            // Register "service_" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *asynchronous* service.
            builder.RegisterService(&service_);
            // Get hold of the completion queue used for the asynchronous communication
            // with the gRPC runtime.
            cq_ = builder.AddCompletionQueue();
            // Finally assemble the server.
            server_ = builder.BuildAndStart();
            std::cout << "Server listening on " << server_address << std::endl;
            // Proceed to the server's main loop.
            if (intersection_srv_parallelism == 1) {
                HandleRpcs();
            }
            omp_set_dynamic(0);
            omp_set_num_threads(intersection_srv_parallelism);
            omp_set_nested(2);
#pragma omp parallel
            {
                HandleRpcs();
            }
        }    
    private:
        // Class encompasing the state and logic needed to serve a request.
        class CallData {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallData(IntersectionService::AsyncService* service, ServerCompletionQueue* cq)
                    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
                        // Invoke the serving logic right away.
                        Proceed();
                    }

                void Proceed() {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        // As part of the initial CREATE state, we *request* that the system
                        // start processing requests. In this request, "this" acts are
                        // the tag uniquely identifying the request (so that different CallData
                        // instances can serve different requests concurrently), in this case
                        // the memory address of this CallData instance.
                        service_->RequestIntersection(&ctx_, &request_, &responder_, cq_, cq_,
                                this);
                    } else if (status_ == PROCESS) {
                        // Spawn a new CallData instance to serve new clients while we process
                        // the one for this CallData. The instance will deallocate itself as
                        // part of its FINISH state.
                        new CallData(service_, cq_);
                        // The actual processing.
                        ProcessRequest(request_, &reply_);
                        // And we are done! Let the gRPC runtime know we've finished, using the
                        // memory address of this instance as the uniquely identifying tag for
                        // the event.
                        status_ = FINISH;
                        responder_.Finish(reply_, Status::OK, this);
                    } else {
                        //GPR_ASSERT(status_ == FINISH);
                        // Once in the FINISH state, deallocate ourselves (CallData).
                        delete this;
                    }
                }
            private:
                // The means of communication with the gRPC runtime for an asynchronous
                // server.
                IntersectionService::AsyncService* service_;
                // The producer-consumer queue where for asynchronous server notifications.
                ServerCompletionQueue* cq_;
                // Context for the rpc, allowing to tweak aspects of it such as the use
                // of compression, authentication, as well as to send metadata back to the
                // client.
                ServerContext ctx_;

                // What we get from the client.
                IntersectionRequest request_;
                // What we send back to the client.
                IntersectionResponse reply_;

                // The means to get back to the client.
                ServerAsyncResponseWriter<IntersectionResponse> responder_;

                // Let's implement a tiny state machine with the following states.
                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;  // The current serving state.
        };

        // This can be run in multiple threads if needed.
        void HandleRpcs() {
            // Spawn a new CallData instance to serve new clients.
            new CallData(&service_, cq_.get());
            void* tag;  // uniquely identifies a request.
            bool ok;
            while (true) {
                // Block waiting to read the next event from the completion queue. The
                // event is uniquely identified by its tag, which in this case is the
                // memory address of a CallData instance.
                // The return value of Next should always be checked. This return value
                // tells us whether there is any kind of event or cq_ is shutting down.
                //GPR_ASSERT(cq_->Next(&tag, &ok));
                cq_->Next(&tag, &ok);
                /*auto r = cq_->AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
                  if (r == ServerCompletionQueue::GOT_EVENT) {
                //GPR_ASSERT(ok);
                static_cast<CallData*>(tag)->Proceed();
                }
                if (r == ServerCompletionQueue::TIMEOUT) continue;*/
                //GPR_ASSERT(ok);
                static_cast<CallData*>(tag)->Proceed();
            }
        }

        std::unique_ptr<ServerCompletionQueue> cq_;
        IntersectionService::AsyncService service_;
        std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
    if (argc == 6) {
        try
        {
            ip_port = argv[1];
            dataset_file_name = argv[2];
            intersection_srv_parallelism = atoi(argv[3]);
            intersection_srv_no = atoi(argv[4]);
            num_intersection_srvs = atoi(argv[5]);
        }
        catch(...)
        {
            CHECK(false, "Enter a valid IP and port number / valid path to dataset/ num of cores: -1 if you want all cores on the machine/ intersection server number / number of intersection servers in the system\n");
        }
    } else {
        CHECK(false, "Format: ./<intersection_server> <IP address:Port Number> <path to dataset> <num of cores: -1 if you want all cores on the machine> <intersection server number> <number of intersection servers in the system>\n");
    }

    CreateIndexFromFile(dataset_file_name, &word_to_docids);

    if (intersection_srv_parallelism == -1) {
        intersection_srv_parallelism = GetNumProcs();
        std::cout << intersection_srv_parallelism << std::endl;
    }

    ServiceImpl server;
    server.Run();

    return 0;
}
