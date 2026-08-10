#pragma once
#include <algorithm>
#include <cstdint>
#include <cstring>

//
// Simple ring buffer, just exposes circular read()/write() calls.
//
// Positions in this buffer maintained by owning objects (e.g. send_stream, recv_stream)
// and simply passed into read()/write(). This is useful when multiple/specific positions
// need to be maintained (more than just r/w), e.g. for send_stream: una_pos, nxt_pos, wr_pos.
//
class ring_buffer {
private:
    uint8_t *buffer_;
    uint64_t capacity_;

public:
    ring_buffer(uint64_t capacity) : capacity_(capacity) {
        buffer_ = new uint8_t[capacity_];
    }

    ~ring_buffer() {
        delete[] buffer_;
        buffer_ = nullptr;
    }

    ring_buffer(const ring_buffer&) = delete;
    ring_buffer &operator=(const ring_buffer&) = delete;

    void read(uint64_t pos, uint8_t *dest, uint64_t n) const {
        uint64_t first = std::min(n, capacity_ - pos);
        std::memcpy(dest, buffer_ + pos, first);
        if (first < n) {
            std::memcpy(dest + first, buffer_, n - first);
        }
    }

    void write(uint64_t pos, uint8_t *src, uint64_t n) {
        uint64_t first = std::min(n, capacity_ - pos);
        std::memcpy(buffer_ + pos, src, first);
        if (first < n) {
            std::memcpy(buffer_, src + first, n - first);
        }
    }

    uint64_t capacity() const { return capacity_; }
};
