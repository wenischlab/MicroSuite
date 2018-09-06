#include <iostream>

#include "intersection.h"

void ComputeIntersection(const std::vector<Docids> &word_one,
        const std::vector<Docids> &word_two,
        std::vector<Docids>* result)
{
    Docids word_one_size = word_one.size();
    Docids word_two_size = word_two.size();

    long i = 0, j = 0;

    while( (i < word_one_size) && (j < word_two_size) )
    {
        if (word_one[i] < word_two[j]) {
            i++;
        } else if (word_two[j] < word_one[i]) {
            j++;
        } else {
            result->emplace_back(word_one[i]);
            i++;
            j++;
        }
    } 
}
