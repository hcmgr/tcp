#pragma once
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>

#include <event2/event.h>

#include "buffer.hpp"
#include "define.hpp"
#include "cong.hpp"
#include "buffer.hpp"

struct timeout_handler;

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
    std::unique_ptr<ring_buffer> buffer_;

    struct segment {
        uint64_t seqnum;
        uint64_t payload_size;
        uint64_t payload_pos;   // position in physical buffer of first byte
    };

    // un-ack'd / in-flight segments
    std::deque<segment> in_flight_segments_;

    std::unique_ptr<congestion_controller> cong_;

    // callback to perform the wire segment send
    using send_segment_cb = std::function<int64_t(uint64_t seqnum, uint16_t flags, uint8_t *payload_ptr, uint64_t payload_len)>;
    send_segment_cb send_segment_cb_;

    struct dup_ack {
        // last 2 valid acks received (to detect triple-dup-ack).
        // 'valid' implies una <= acknum, otherwise we just drop it.
        uint64_t last_acks[2];

        dup_ack() { last_acks[0] = 0; last_acks[1] = 0; }
    };
    dup_ack dup_ack_;

    std::unique_ptr<timeout_handler> retransmission_timeout_;

public:
    send_stream(uint64_t capacity, send_segment_cb send_segment_cb);
    ~send_stream();

public:
    // handshake syn sent by connection (advance nxt)
    int64_t on_syn_sent();

    // user write new bytes to send
    int64_t write(uint64_t n, uint8_t *src_buffer);

    // peer ack'd our stream
    int64_t on_ack_recv(uint64_t acknum);

public:
    uint64_t ready_bytes();
    uint64_t free_space_bytes();

    uint64_t get_cwnd() { return cong_->get_cwnd(); }
    uint64_t get_nxt() { return nxt_; }
    std::string to_string() { return ""; }

public:
    void on_retransmission_timeout();

private:
    //
    // Send ready bytes (nxt onwards) on new data avail, i.e. on:
    //      a) user write, or;
    //      b) ack recv
    //
    int64_t send_ready_bytes();

    // triggered by a congestion event (rto or triple dup ack)
    int64_t retransmit_oldest_segment();

private:
    uint64_t inc(uint64_t pos, uint64_t n) const { return (pos + n) % buffer_->capacity(); }
};