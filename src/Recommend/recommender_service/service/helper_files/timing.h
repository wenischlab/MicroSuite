#ifndef __TIMING_H_INCLUDED__
#define __TIMING_H_INCLUDED__

#pragma once
#include <cstdint>
#include <sys/time.h>

uint64_t GetTimeInMicro();
void GetCpuTimes(uint64_t* user_time,
        uint64_t* system_time,
        uint64_t* io_time,
        uint64_t* idle_time);

uint64_t GetTimeInSec();

#endif //__TIMING_H_INCLUDED__
