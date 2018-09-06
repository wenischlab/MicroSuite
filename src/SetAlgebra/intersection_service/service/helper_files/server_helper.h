#ifndef __SERVER_HELPER_H_INCLUDED__
#define __SERVER_HELPER_H_INCLUDED__

#include <fstream>
#include <sstream>

#include "intersection_service/src/intersection.h"
#include "protoc_files/intersection.grpc.pb.h"

void CreateIndexFromFile(const std::string dataset_file_name,
        std::map<Docids, std::vector<Docids> >* index);

void UnpackIntersectionServiceRequest(const intersection::IntersectionRequest &request,
        std::vector<Docids>* word_ids);

bool ExtractDocids(const std::vector<Docids> &word_ids,
        const std::map<Docids, std::vector<Docids> > &word_to_docids,
        std::vector<std::vector<Docids> >* doc_ids_for_all_words);

void PackIntersectionServiceResponse(const std::vector<Docids> &intersection_res,
        intersection::IntersectionResponse* reply);

#endif //__SERVER_HELPER_H_INCLUDED__
