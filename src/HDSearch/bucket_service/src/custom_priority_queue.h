#ifndef __CUSTOM_PRIORITY_QUEUE_INCLUDED__
#define __CUSTOM_PRIORITY_QUEUE_INCLUDED__

#include <queue>
#include "multiple_points.h"

typedef std::pair<uint32_t, float> PointIDDistPair;
// Create top element of priority queue based on the second element of a pair.
class CompareDist
{
    public:
        bool operator()(PointIDDistPair n1, PointIDDistPair n2)
        {
            return n1.second < n2.second;
        }
};

// Class implementation right now, because we might change the data structure.
class CustomPriorityQueue
{
    public:
        CustomPriorityQueue() = default;
        // Checks & returns true if the priority queue is empty.
        bool IsEmpty() const;
        // Returns the size of the priority queue.
        unsigned GetSize() const;
        // Adds a pair of <Point, dist> to the priority queue.
        void AddToPriorityQueue(PointIDDistPair &knn_point);
        // Removes the pair <Point, dist> with the largest distance from query.
        void RemoveTopElement();
        // Returns the Point with the largest distance from query.
        uint32_t GetTopPointID() const;
        // Returns the largest distance from query.
        float GetTopDistance() const;
    private:
        // Priority queue of a pair of the Point and its distance from query.
        std::priority_queue<PointIDDistPair, 
            std::vector<PointIDDistPair>, 
            CompareDist> knn_priority_queue_;
};
#endif //__CUSTOM_PRIORITY_QUEUE_INCLUDED__



