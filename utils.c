//
// Created by Stefan on 13/03/2023.
//
#include "utils.h"
#define NANOS_IN_SECOND 1000000000

struct timeval getCurrentTime()
{
    struct timeval ts;
    gettimeofday(&ts, NULL);
    return ts;
}

long long timediff_microseconds(struct timeval start, struct timeval end) {
    long long sec1 = start.tv_sec;
    long long sec2 = end.tv_sec;
    long long ns1 = start.tv_usec;
    long long ns2 = end.tv_usec;

    if (ns2 > ns1) {
        return (sec2 - sec1) * 1000000 + (ns2 - ns1) / 1000;
    } else {
        return (sec2 - sec1 - 1) * 1000000 + (NANOS_IN_SECOND + ns2 - ns1) / 1000;
    }
}

