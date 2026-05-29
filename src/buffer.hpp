#include <vector>
#include <atomic>
#include <cstring>
#include <cerrno>

#include "utils.hpp"

#define DEFAULT_CAPACITY 4096

struct RingBuffer {
    uint8_t *buffer;
    int64_t capacity;

    int64_t readPos;
    int64_t writePos;

    RingBuffer(int64_t c)
        : capacity(c), readPos(0), writePos(0)
    {
        buffer = (uint8_t*)malloc(capacity);
    }

    RingBuffer() 
        : RingBuffer(DEFAULT_CAPACITY) {}
    
    ~RingBuffer() {
        free(buffer);
    }

    int64_t readN(int n, uint8_t *outBuffer) {
        if (readAvail() < n) {
            return -1;
        }
        memcpy(outBuffer, buffer + readPos, n);
        readPos = (readPos + n) % capacity;
        return n;
    }

    int64_t writeN(int n, uint8_t *inBuffer) {
        if (freeSpace() < n) {
            return -1;
        }
        memcpy(buffer + writePos, inBuffer, n);
        writePos = (writePos + n) % capacity;
        return n;
    }

    int64_t readAvail() {
        return ((writePos + capacity) - readPos) % capacity;
    }

    int64_t freeSpace() {
        return ((readPos + capacity) - writePos) % capacity;
    }
};