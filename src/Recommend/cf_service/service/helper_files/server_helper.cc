#include <algorithm>
#include <fstream>
#include <math.h>
#include "server_helper.h"
#include <stdlib.h>

using namespace mlpack;
using namespace mlpack::cf;

std::mutex test;


void CreateDatasetFromFile(std::string dataset_file_name, 
        Matrix* dataset)
{   
    data::Load(dataset_file_name, *dataset);
}

void UnpackCFServiceRequest(const collaborative_filtering::CFRequest &request,
        Request* user_item)
{
    user_item->user = request.user();
    user_item->item = request.item();
}

void CalculateRating(const Request &user_item,
        CF* cf,
        float* rating)
{
    *rating = cf->Predict(user_item.user, user_item.item);
}

void PackCFServiceResponse(const float rating,
        collaborative_filtering::CFResponse* reply)
{
    reply->set_rating(rating);
}
