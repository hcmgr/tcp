#pragma once
#include <cstdint>
#include <deque>
#include <memory>

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

    std::weak_ptr<connection> conn_ref;

    // triple-dup-ack state
    struct triple_dup_ack {
        uint64_t last_acks[2];
    };

    // rto state
    struct rto {
        bool active;
        struct event *ev;
    };

    // delayed-ack state
    struct delayed_ack {
        bool active;
        struct event *ev;
    };

public:
    send_stream(uint64_t capacity);
    ~send_stream();

public:
    // handshake syn sent by connection
    int64_t on_syn_sent();

    // user write new bytes to send
    int64_t write(uint64_t n, uint8_t *src_buffer);

    // peer ack'd our stream
    int64_t on_ack_recv(uint64_t acknum);

public:
    uint64_t ready_bytes();
    uint64_t free_space_bytes();
    uint64_t nxt() { return nxt_; }
    std::string to_string() { return ""; }

private:
    //
    // Send ready bytes (nxt onwards) on new data avail, i.e. on:
    //      a) user write, or;
    //      b) ack recv
    //
    void send_ready_bytes();

    // triggered by a congestion event (rto or triple dup ack)
    void retransmit_oldest_segment();

    // congestion event handlers
    void on_rto();
    void on_triple_dup_ack();

private:
    uint64_t inc(uint64_t pos, uint64_t n) const { return (pos + n) % buffer_->capacity(); }
};