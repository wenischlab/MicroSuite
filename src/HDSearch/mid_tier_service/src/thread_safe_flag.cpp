#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

template <typename T>
class ThreadSafeFlag
{
    public:

        void Wait()
        {
            std::unique_lock<std::mutex> mlock(mutex_);
            while (!flag_)
            {
                cond_.wait(mlock);
            }
        }

        void Set()
        {
            std::unique_lock<std::mutex> mlock(mutex_);
            flag_ = true;
            mlock.unlock();
            cond_.notify_all();
        }

        void Reset()
        {
            std::unique_lock<std::mutex> mlock(mutex_);
            flag_ = false;
            mlock.unlock();
        }


    private:
        bool flag_;
        std::mutex mutex_;
        std::condition_variable cond_;
};
