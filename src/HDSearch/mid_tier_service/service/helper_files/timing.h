#pragma once
#include <cstdint>
#include <x86intrin.h>
#include <sstream>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/types.h>
#include <unistd.h>

uint64_t GetTimeInMicro();
uint64_t GetTimeInSec();
double Rdtsc();
uint64_t RdtscCycles();
void GetCpuTimes(uint64_t* user_time,
        uint64_t* system_time,
        uint64_t* io_time,
        uint64_t* idle_time);

