/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <iostream>
#include <memory>
#include <omp.h>
#include <string>
#include <sys/time.h>
#include <grpc++/grpc++.h>

#include "lookup_service/service/helper_files/server_helper.h"
#include "lookup_service/service/helper_files/timing.h"
#include "lookup_service/service/helper_files/utils.h"

#define NODEBUG

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using lookup::Key;
using lookup::UtilRequest;
using lookup::TimingDataInMicro;
using lookup::UtilResponse;
using lookup::Value;
using lookup::LookupService;

std::string ip_port = "";
memcached_st *memc;
memcached_return rc;
std::mutex bucket_mutex;
int lookup_srv_parallelism = 1, lookup_server_no = 0, memcached_port = 11211;

void ProcessRequest(Key &request,
        Value* reply)
{
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
       so as to calculate the CPU util when the bucket is done.*/
    size_t idle_time_initial = 0, total_time_initial = 0, idle_time_final = 0, total_time_final = 0;
    //GetCpuTimes(&idle_time_initial, &total_time_initial);
    uint64_t start_time = 0, end_time = 0;

    // Unpack received queries and point IDs
    std::string key = "", value = "";
    uint32_t operation = 0;
    start_time = GetTimeInMicro();
    UnpackBucketServiceRequest(request,
            &operation,
            &key,
            &value);
    end_time = GetTimeInMicro();
    reply->mutable_timing_data_in_micro()->set_unpack_lookup_srv_req_time_in_micro((end_time - start_time));
#ifndef NODEBUG
    std::cout << "after unpack\n";
#endif

    start_time = GetTimeInMicro();
    /* Next perform the get, set, or update
       operation for the request received.*/
    switch(operation)
    {
        case 1:
            {
                Get(memc,
                        &rc,
                        key,
                        &value);
                reply->set_value(value);
                break;
            }
        case 2:
            {
                Set(memc,
                        &rc,
                        key,
                        &value);

                reply->set_value(value);
                break;
            }
        case 3:
            {
                Update(key, value);
                break;
            }
    }

#ifndef NODEBUG
    std::cout << "after getting or setting\n";
#endif
    reply->mutable_timing_data_in_micro()->set_lookup_srv_time_in_micro((GetTimeInMicro() - start_time));


    // Convert K-NN into form suitable for GRPC.
    const float idle_time_delta = idle_time_final - idle_time_initial;
    const float total_time_delta = total_time_final - total_time_initial;
    const float cpu_util = (100.0 * (1.0 - (idle_time_delta/total_time_delta)));
    reply->mutable_timing_data_in_micro()->set_cpu_util(cpu_util);

#ifndef NODEBUG
    std::cout << "setting status as ok\n";
#endif
    reply->set_send_stamp(GetTimeInMicro());
    reply->set_index_view(request.index_view());
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

            // Then create a memcached conn with extracted ip and port number.
            memc = memcached_create(NULL);
            CreateMemcachedConn(memcached_port, memc, &rc);

            // Proceed to the server's main loop.
            if (lookup_srv_parallelism == 1) {
                HandleRpcs();
            }
            omp_set_dynamic(0);
            omp_set_num_threads(lookup_srv_parallelism);
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
                CallData(LookupService::AsyncService* service, ServerCompletionQueue* cq)
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
                        service_->RequestKeyLookup(&ctx_, &request_, &responder_, cq_, cq_,
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
                LookupService::AsyncService* service_;
                // The producer-consumer queue where for asynchronous server notifications.
                ServerCompletionQueue* cq_;
                // Context for the rpc, allowing to tweak aspects of it such as the use
                // of compression, authentication, as well as to send metadata back to the
                // client.
                ServerContext ctx_;

                // What we get from the client.
                Key request_;
                // What we send back to the client.
                Value reply_;

                // The means to get back to the client.
                ServerAsyncResponseWriter<Value> responder_;

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
        LookupService::AsyncService service_;
        std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
    if (argc == 5) {
        try
        {
            ip_port = argv[1];
            memcached_port = atoi(argv[2]);
            lookup_srv_parallelism = atoi(argv[3]);
            lookup_server_no = atoi(argv[4]);
        }
        catch(...)
        {
            CHECK(false, "Enter a valid IP and port number, valid memcached port number, number of cores for this lookup server - enter minus 1 if you want all cores, lookup server number\n");
        }
    } else {
        CHECK(false, "Format: ./<memcached_server> <IP address:Port Number> <Memcached port number to connect to> <number of cores - minus one if you want all cores on this machine> <lookup server number>\n");
    }

    if (lookup_srv_parallelism == -1) {
        lookup_srv_parallelism = GetNumProcs();
    }
    ServiceImpl server;
    server.Run();

    return 0;
}
