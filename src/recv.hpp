#include <deque>

#include "buffer.hpp"
#include "define.hpp"
#include "buffer.hpp"

class recv_stream {
private:
    // logical seqnum state
    uint64_t irs_;
    uint64_t nxt_;
    uint64_t fin_;

    // physical buffer offsets
    uint64_t nxt_pos_;
    uint64_t rd_pos_;

    // physical buffer
    ring_buffer *buffer_;

    struct segment {
        uint64_t seqnum;
        uint64_t payload_size;
        uint64_t payload_pos;   // position in physical buffer of first byte
    };

    // received segments non-contiguous with nxt
    std::deque<segment> pending_segments_;

    enum class state {
        SYN_WAITING,
        ESTABLISHED,
        FINISHED
    };
    state state_;
    
public:
    recv_stream(uint64_t capacity);
    ~recv_stream();

public:
    // receive segment payload from peer
    int64_t recv_segment(uint64_t seqnum, uint8_t *payload_ptr, uint64_t payload_size);

    // user read for `n` available bytes
    int64_t read(uint64_t n, uint8_t *dest_buffer);

    void on_syn(uint64_t irs);
    void on_fin(uint64_t fin);

    int64_t ready_bytes();
    int64_t free_space_bytes();

private:
    uint64_t inc(uint64_t pos, uint64_t n) const { return (pos + n) % buffer_->capacity(); }
};