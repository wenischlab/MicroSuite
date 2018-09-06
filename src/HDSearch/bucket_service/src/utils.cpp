#include <thread>

unsigned int GetNumProcs()
{
    return std::thread::hardware_concurrency();
}
