#ifndef __MULTIPLE_POINTS_H_INCLUDED__
#define __MULTIPLE_POINTS_H_INCLUDED__
#include <algorithm>
#include <assert.h>
#include <fstream>
#include <set>
#include <sstream>
#include <string.h>
#include "point.h"

class MultiplePoints
{
    public:
        MultiplePoints() = default;
        // Second constructor to initialize "size"# points to a value.
        MultiplePoints(int size, const Point &p)
        {
            multiple_points_.assign(size, p);
        }
        /* Read text file and create a structure: vector of points. 
           file format: float11 float12 float13 ... --> all dimensions of pt 1.
           float21 float22 float23 ... --> all dimensions of pt 2.
           In: path to text file.
           Out: multiple_points_ private variable takes up point values
           based on the text file.*/
        void CreateMultiplePoints(const std::string &file_name);
        void CreateMultiplePointsFromBinFile(const std::string &file_name);
        // Dimension of all points in dataset/queries must be the same.
        void ValidateDimensions(const int dataset_dimension, 
        const int point_dimension) const;
        /* Read float elements of a line and store them as point dimensions.
        In: a set of string elements (represents floats of point dimensions).
        Out: a variable of type "Point" - has floats for all dimensions.*/
        void CreatePoint(const std::vector<std::string> &tokens, Point* p) ;
        // Return total number of points in this collection.
        unsigned GetSize() const;
        /* Resize a set of multiplepoints.
In: new size, new points
Out: the multiplepoints private member gets modified.*/
        void Resize(const int size, const Point &p);
        // Return the dimension of each point (points must have equal dimension).
        unsigned GetPointDimension() const;
        // Remove all points, makes data structure empty.
        void Clear();
        // Get the Point at a given index.
        const Point& GetPointAtIndex(const int index) const;
        // Add a point to the end of the vector.
        void PushBack(const Point &point);
        // Get point at back.
        Point& GetPointAtBack();
        
        // Add point to a given index.
        void SetPoint(const unsigned int index, const Point &point);
        std::vector<Point>::iterator Begin();
        void Erase(std::vector<Point>::iterator element);
        void PopBack();
        // Prints a collection of points.
        void Print() const;
    private:
        // Define a data structure that stores a collection of points.
        std::vector<Point> multiple_points_;
};
#endif // __MULTIPLE_POINTS_H_INCLUDED__
