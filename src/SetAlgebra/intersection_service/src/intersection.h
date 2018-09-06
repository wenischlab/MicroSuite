#ifndef __INTERSECTION_H_INCLUDED__
#define __INTERSECTION_H_INCLUDED__

#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}

#include <vector>

typedef uint32_t Docids;

void ComputeIntersection(const std::vector<Docids> &word_one,
        const std::vector<Docids> &word_two,
        std::vector<Docids>* result);

#endif // __INTERSECTION_H_INCLUDED__
