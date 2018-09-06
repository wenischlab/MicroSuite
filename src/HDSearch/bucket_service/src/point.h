#ifndef __POINT_H_INCLUDED__
#define __POINT_H_INCLUDED__
#include <iostream>
#include <vector>

#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}

// Class to define/access a Point: floats for each dimension.
class Point
{
    public:
        // Point constructor: initializes a point with one float value: 0.0
        Point() = default;
        // To initialize point with "size" dimensions, each dim = value.
        Point(const int size, const float &value)
        {
            point_.assign(size, value);
        }
        // Resize a point.
        void Resize(const int size, const float value);
        // Add a float to a particular dimension (index).
        void AddValueToIndex(const int index, const float value);
        // Returns the float at a particular dimension (index).
        float GetValueAtIndex(const int index) const;
        /* Given a float array, create a point from it.
           Helps when reading a binary dataset file and creating points.
In: float array
Out: the point object itself.*/
        void CreatePointFromFloatArray(const float float_arr[], 
                const int float_arr_size);
        // Returns the number of floats in the point (dimension).
        unsigned GetSize() const; 
        // Check if two points are equal.
        bool Equal(const Point &p) const;
        // Prints a point to terminal.
        void PrintPoint() const;
        // private:
        // Defines the data structure that holds collection of floats i.e point.
        std::vector<float> point_;
};
#endif // __POINT_H_INCLUDED__.
