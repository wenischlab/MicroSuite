#include <iterator>
#include "multiple_points.h"

// Read input file and create dataset.
void MultiplePoints::CreateMultiplePoints(const std::string &file_name)
{
    std::ifstream file(file_name);
    // If file does not exist, error out.
    CHECK((file.good()), "Cannot create multiple points from file because file does not exist\n");
    std::string line;
    int dimension = 0, curr_dimension = 0;

    // Process each file line.
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buf(line);
        std::istream_iterator<std::string> begin(buf), end;
        std::vector<std::string> tokens(begin, end);

        // Get the dimension of the first point.
        if (i == 0) {
            dimension = tokens.size();
        }

        // Current dimension must match the first point.
        CHECK((dimension == tokens.size()), "ERROR: Dimensions of all points (dataset & queries) must be equal\n");

        // Read values and return a point at the end of multiple_points_.
        multiple_points_.push_back(Point());
        CreatePoint(tokens, &multiple_points_.back());
    }

}

void MultiplePoints::CreateMultiplePointsFromBinFile(const std::string &file_name)
{
    int dataset_dimensions = 2048;
    FILE *dataset_binary = fopen(file_name.c_str(), "r");
    if(!dataset_binary)
    {
        CHECK(false, "ERROR: Could not open dataset file\n");
    }
    /* Get the size of the file, so that we can initialize number
       of points, given 2048 dimensions.*/
    std::ifstream stream_to_get_size(file_name, std::ios::binary | std::ios::in);
    stream_to_get_size.seekg(0, std::ios::end);
    long size = stream_to_get_size.tellg();
    long dataset_size = (size/sizeof(float))/dataset_dimensions;
    CHECK((dataset_size >= 0), "ERROR: Dataset cannot have negative number of points\n");
    CHECK((dataset_dimensions >= 0), "ERROR: Number of dataset dimensions cannot be negative\n");

    //Read each individual float value and store it in the dataset matrix.
    float value;
    Point p(dataset_dimensions, 0.0);
    multiple_points_.resize(dataset_size, p);

    for(long m = 0; m < dataset_size; m++)
    {
        for(int n = 0; n < dataset_dimensions; n++)
        {
            if(fread((void*)(&value), sizeof(value), 1, dataset_binary) == 1)
            {
                multiple_points_[m].AddValueToIndex(n, static_cast<float>(value));
            } else {
                break;
            }
        }
    }

}

void MultiplePoints::ValidateDimensions(const int dataset_dimension, 
        const int point_dimension) const
{
    // If the dimensions are unequal, exit with exception.
    CHECK((dataset_dimension == point_dimension), "Dimensions of all points (dataset & queries) must be equal");

}

void MultiplePoints::CreatePoint(const std::vector<std::string> &tokens, 
        Point* p)
{
    float dim_value = 0.0;
    size_t sz = 0;
    int itr = 0;

    // Set a size to the empty point.
    p->Resize(tokens.size(), 0.0);

    for(auto& s: tokens)
    {
        // All values read from be floats, else catch exception.
        try
        {
            dim_value = stof(s, &sz);
            // Create point based on real dimension values.
            p->AddValueToIndex(itr, dim_value);
        }
        catch(...)
        {
            CHECK(false, "Values must be real numbers");

        }
        itr++;
    }

}

unsigned MultiplePoints::GetSize() const
{
    return multiple_points_.size();
}

void MultiplePoints::Resize(const int size, const Point &p)
{
    multiple_points_.resize(size, p);
}

unsigned MultiplePoints::GetPointDimension() const
{
    /* Assumption: All points have equal dimension -
       because this is checked for while loading the file. */
    CHECK((multiple_points_.size() != 0), "Dataset/Queries cannot be an empty file");
    return multiple_points_[0].GetSize();
}

void MultiplePoints::Clear()
{
    multiple_points_.clear();
}

const Point& MultiplePoints::GetPointAtIndex(const int index) const
{
    return multiple_points_[index];
}

void MultiplePoints::PushBack(const Point &point)
{
    multiple_points_.emplace_back(point);
}

Point& MultiplePoints::GetPointAtBack()
{
    return multiple_points_.back();
}

void MultiplePoints::SetPoint(const unsigned int index, 
        const Point &point)
{
    CHECK((multiple_points_.size() >= index), "ERROR: Trying to add point to a non-existent MultiplePoints index\n");
    multiple_points_[index] = point;
}

void MultiplePoints::Erase(std::vector<Point>::iterator element)
{
    multiple_points_.erase(element);
}

void MultiplePoints::PopBack()
{
    multiple_points_.pop_back();
}

std::vector<Point>::iterator MultiplePoints::Begin()
{
    return multiple_points_.begin();
}

void MultiplePoints::Print() const
{
    for(auto& point : multiple_points_)
    {
        point.PrintPoint();
        std::cout << std::endl;
    }
}
