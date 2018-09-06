#include "custom_priority_queue.h"

bool CustomPriorityQueue::IsEmpty() const
{
    return knn_priority_queue_.empty();
}

unsigned CustomPriorityQueue::GetSize() const
{
    return knn_priority_queue_.size();
}

void CustomPriorityQueue::AddToPriorityQueue(PointIDDistPair &knn_point)
{
    knn_priority_queue_.push(knn_point);
}

void CustomPriorityQueue::RemoveTopElement()
{
    CHECK(!knn_priority_queue_.empty(), "ERROR: Tring to remove an element from a NULL data structure\n"); 
        knn_priority_queue_.pop();
}

uint32_t CustomPriorityQueue::GetTopPointID() const
{
    CHECK(!knn_priority_queue_.empty(), "ERROR: Tring to get an element from a NULL data structure\n"); 
        return knn_priority_queue_.top().first;
}

float CustomPriorityQueue::GetTopDistance() const
{
    CHECK(!knn_priority_queue_.empty(), "ERROR: Tring to get an element from a NULL data structure\n"); 
        return  knn_priority_queue_.top().second;
}
