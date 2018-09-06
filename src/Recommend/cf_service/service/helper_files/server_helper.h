#include <iostream>
#include <stdio.h>
#include <string>
#include <mlpack/core.hpp>
#include <mlpack/methods/cf/cf.hpp>

#include "protoc_files/cf.grpc.pb.h"

#ifndef __CLIENT_HELPER_H_INCLUDED__
#define __CLIENT_HELPER_H_INCLUDED__
#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}

#define EMPTY -2.0

typedef arma::mat Matrix;

struct Request {
    int user = 0;
    int item = 0;
};

void CreateDatasetFromFile(const std::string dataset_file_name, 
        Matrix* dataset);

void UnpackCFServiceRequest(const collaborative_filtering::CFRequest &request,
        Request* user_item);

void CalculateRating(const Request &user_item,
        mlpack::cf::CF* cf_matrix,
        float* rating);

void PackCFServiceResponse(const float rating,
        collaborative_filtering::CFResponse* reply);

#endif // __CLIENT_HELPER_H_INCLUDED__

