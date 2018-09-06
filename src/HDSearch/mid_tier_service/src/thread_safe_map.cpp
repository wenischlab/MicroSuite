#include <iostream>
#include <mutex>  // For std::unique_lock
#include <shared_mutex>
#include <thread>
#include <map>

class ThreadSafeMap {
    public:
        ThreadSafeMap() = default;

        // Multiple threads/readers can read the counter's value at the same time.
        std::string Get(std::string key) {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            try {
                map_.at(key);
            } catch( ... ) {
                return "nack";
            }
            return map_[key];
        }

        // Only one thread/writer can increment/write the counter's value.
        void Set(std::string key, std::string value) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            map_[key] = value;
        }

    private:
        mutable std::shared_mutex mutex_;
        std::map<std::string, std::string> map_;
};

