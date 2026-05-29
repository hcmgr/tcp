#include <cstdint>

#include "buffer.hpp"

struct SendStream {
    RingBuffer buffer;

    int64_t SND_ISS;
    int64_t SND_UNA;
    int64_t SND_WND;
    int64_t SND_NXT;

    int64_t readN();
    int64_t writeN();

    SendStream();

};

struct RecvStream {
    RingBuffer buffer;

    int64_t RCV_ISS;
    int64_t RCV_NXT;
    int64_t RCV_WND;

    int64_t readN();
    int64_t writeN();

    RecvStream();

    int64_t freeSpace() {
        return buffer.freeSpace();
    }
};