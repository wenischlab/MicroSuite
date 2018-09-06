#include "point.h"

void Point::Resize(const int size, const float value)
{
    point_.assign(size, value);
}
void Point::AddValueToIndex(const int index, const float value)
{
    point_[index] = value;
}

float Point::GetValueAtIndex(const int index) const
{
    return point_[index];
}

void Point::CreatePointFromFloatArray(const float float_arr[], 
        const int float_arr_size)
{
    CHECK((point_.size() == float_arr_size), "ERROR: Size of point must be initialized to the size of float array\n");
    for(int i = 0; i < float_arr_size; i++)
    {
        point_[i] = float_arr[i];
    }
}

unsigned Point::GetSize() const
{
    return point_.size();
}

bool Point::Equal(const Point &p) const
{
    if (point_.size() != p.GetSize()) 
    {
        return false;
    }
    return std::equal(point_.begin(), point_.end(), p.point_.begin());
}

void Point::PrintPoint() const
{
    for(auto& p : point_)
    {
        std::cout << p << " ";
    }
}
