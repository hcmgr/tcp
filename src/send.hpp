#pragma once
#include <cstdint>
#include <deque>

#include "buffer.hpp"
#include "define.hpp"
#include "cong.hpp"
#include "buffer.hpp"

class send_stream {
private:
    // logical seqnums
    uint64_t iss_;
    uint64_t una_;
    uint64_t nxt_;
    uint64_t fin_;

    // physical buffer offsets
    uint64_t una_pos_;
    uint64_t nxt_pos_;
    uint64_t wr_pos_;

    // physical buffer
    ring_buffer *buffer_;

    struct segment {
        uint64_t seqnum;
        uint64_t payload_size;
        uint64_t payload_pos;   // position in physical buffer of first byte
    };

    // un-ack'd / in-flight segments
    std::deque<segment> in_flight_segments_;

    congestion_controller *cong_;

    // delayed-ack state
    struct delayed_ack {

    };

    // delayed-send state
    struct delayed_send {

    };

    // rto state
    struct rto {

    };

    // triple-dup-ack state
    struct triple_dup_ack {
        uint64_t last_acks[2];
    };
    

public:
    send_stream(uint64_t capacity);
    ~send_stream();

public:
    // header-only sends
    void send_syn();
    void send_syn_ack();
    void send_ack();
    void send_fin();
    void send_rst();

    // user write new bytes to send
    int64_t write(uint64_t n, uint8_t *src_buffer);

    // peer ack'd our stream
    int64_t on_ack(uint64_t acknum);

    uint64_t ready_bytes();
    uint64_t free_space_bytes();

private:
    // on new data avail (write or on_ack) or congestion event (rto or triple dup ack)
    void send_ready_bytes();

    // on congestion event (rto or triple dup ack)
    void retransmit_oldest_segment();

    // congestion events
    void on_rto();
    void on_triple_dup_ack();

private:
    uint64_t inc(uint64_t pos, uint64_t n) const { return (pos + n) % buffer_->capacity(); }
};