#include "server_helper.h"

#define BASE (10)

void CreateIndexFromFile(const std::string dataset_file_name,
        std::map<Docids, std::vector<Docids> >* index)
{
    std::ifstream file(dataset_file_name);
    // If file does not exist, error out.
    CHECK((file.good()), "Cannot create words_ids->docids index from file because file does not exist\n");
    std::string line;
    Docids token_no = 0, word_id = 0, doc_id = 0;
    char* err_check;

    // Process each file line.
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buf(line);
        std::istream_iterator<std::string> begin(buf), end;
        std::vector<std::string> tokens(begin, end);

        /* Read values and return a set of doc 
           ids corresponding to the word id.*/
        token_no = 0;

        // Set a size to the empty point.

        for(auto& s: tokens)
        {
            // All values read from be floats, else catch exception.
            if(token_no == 0) {
                word_id = strtol(s.c_str(), &err_check, BASE);
            } else {
                if (token_no == 1) {
                    std::vector<Docids> docids;
                    docids.emplace_back(strtol(s.c_str(), &err_check, BASE));
                    (*index)[word_id] = docids;
                } else {
                    index->at(word_id).emplace_back(strtol(s.c_str(), &err_check, BASE));
                }
            }
            token_no++;
        }
    }
}

void UnpackIntersectionServiceRequest(const intersection::IntersectionRequest &request,
        std::vector<Docids>* word_ids)
{
    int num_ids = request.word_ids_size();
    Docids id_value = 0;

    for(int i = 0; i < num_ids; i++) {
        id_value = request.word_ids(i);
        word_ids->emplace_back(id_value);
    }
}

bool ExtractDocids(const std::vector<Docids> &word_ids,
        const std::map<Docids, std::vector<Docids> > &word_to_docids,
        std::vector<std::vector<Docids> >* doc_ids_for_all_words)
{
    Docids num_words_in_query = word_ids.size();

    for(Docids i = 0; i < num_words_in_query; i++) {
        try {
            word_to_docids.at(word_ids[i]);
        } catch(...) {
            return false;
            //CHECK(false, "Word not present in wikipedia\n");
            /* Return result for other words if this
               word if this word does not exist in wikipedia.*/
            continue;
        }
        // AS HACK
        if (word_to_docids.at(word_ids[i]).size() > 1000)
        {
            continue;
        }
        doc_ids_for_all_words->emplace_back(word_to_docids.at(word_ids[i]));
    }
    /* If all words must have doc ids, then you should
       activate the following invariant.*/
    //CHECK( (doc_ids_for_all_words->size() == num_words_in_query), "Could not populate doc ids for all words in the query\n");
    return true;
}

void PackIntersectionServiceResponse(const std::vector<Docids> &intersection_res,
        intersection::IntersectionResponse* reply)
{
    Docids intersection_res_size = intersection_res.size();

    for(Docids i = 0; i < intersection_res_size; i++)
    {
        reply->add_doc_ids(intersection_res[i]);

    }
}
