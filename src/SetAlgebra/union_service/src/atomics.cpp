#include <atomic>

enum TMNames {sip1, sdp1_20, sdb1_50};
enum AsyncTMNames {aip1_0_1, adp1_4_1, adb1_4_4};
enum FMSyncTMNames {sdb20_1, sdb20_50, last_sync};
enum FMAsyncTMNames {adb1_1_1, adb4_4_4, last_async};

class Atomics
{
    public:
        Atomics(): count_(0), flag_(false), tm_name_(sip1), async_tm_name_(aip1_0_1), fm_sync_tm_name_(sdb20_1), fm_async_tm_name_(adb1_1_1){}
        bool AtomicallyReadFlag()
        {
            bool flag = flag_.load(std::memory_order_acquire);
            return flag;
        }

        void AtomicallySetFlag(bool flag_value)
        {
            flag_.store(flag_value, std::memory_order_release);
        }

        int AtomicallyReadCount()
        {
            int local_cnt = count_.load(std::memory_order_acquire);
            return local_cnt;
        }

        void AtomicallyResetCount()
        {
            int reset_val = 0;
            count_.store(reset_val, std::memory_order_release);
        }

        int AtomicallyIncrementCount()
        {
            int local_cnt = 0;
            do {
                local_cnt = count_.load(std::memory_order_acquire);
            } while (!count_.compare_exchange_weak(local_cnt, local_cnt + 1, std::memory_order_acq_rel, std::memory_order_relaxed));
            return local_cnt + 1;
        }

        int AtomicallyDecrementCount()
        {
            int local_cnt = 0;
            do {
                local_cnt = count_.load(std::memory_order_acquire);
            } while (!count_.compare_exchange_weak(local_cnt, local_cnt - 1, std::memory_order_acq_rel, std::memory_order_relaxed));
            return local_cnt - 1;
        }

        TMNames AtomicallyReadTM()
        {
            TMNames tm_name = tm_name_.load(std::memory_order_acquire);
            return tm_name;
        }

        AsyncTMNames AtomicallyReadAsyncTM()
        {
            AsyncTMNames tm_name = async_tm_name_.load(std::memory_order_acquire);
            return tm_name;
        }

        FMSyncTMNames AtomicallyReadFMSyncTM()
        {
            FMSyncTMNames tm_name = fm_sync_tm_name_.load(std::memory_order_acquire);
            return tm_name;
        }

        FMAsyncTMNames AtomicallyReadFMAsyncTM()
        {
            FMAsyncTMNames tm_name = fm_async_tm_name_.load(std::memory_order_acquire);
            return tm_name;
        }

        TMNames AtomicallySetTM(TMNames new_tm)
        {
            TMNames old_tm;
            do {
                old_tm = tm_name_.load(std::memory_order_acquire);
            } while (!tm_name_.compare_exchange_weak(old_tm, new_tm, std::memory_order_acq_rel, std::memory_order_relaxed));
            return new_tm;
        }

        AsyncTMNames AtomicallySetAsyncTM(AsyncTMNames new_tm)
        {
            AsyncTMNames old_tm;
            do {
                old_tm = async_tm_name_.load(std::memory_order_acquire);
            } while (!async_tm_name_.compare_exchange_weak(old_tm, new_tm, std::memory_order_acq_rel, std::memory_order_relaxed));
            return new_tm;
        }

        FMSyncTMNames AtomicallySetFMSyncTM(FMSyncTMNames new_tm)
        {
            FMSyncTMNames old_tm;
            do {
                old_tm = fm_sync_tm_name_.load(std::memory_order_acquire);
            } while (!fm_sync_tm_name_.compare_exchange_weak(old_tm, new_tm, std::memory_order_acq_rel, std::memory_order_relaxed));
            return new_tm;
        }

        FMAsyncTMNames AtomicallySetFMAsyncTM(FMAsyncTMNames new_tm)
        {
            FMAsyncTMNames old_tm;
            do {
                old_tm = fm_async_tm_name_.load(std::memory_order_acquire);
            } while (!fm_async_tm_name_.compare_exchange_weak(old_tm, new_tm, std::memory_order_acq_rel, std::memory_order_relaxed));
            return new_tm;
        }

    private:
        std::atomic<int> count_;
        std::atomic<bool> flag_;
        std::atomic<TMNames> tm_name_;
        std::atomic<AsyncTMNames> async_tm_name_;
        std::atomic<FMSyncTMNames> fm_sync_tm_name_;
        std::atomic<FMAsyncTMNames> fm_async_tm_name_;
};
