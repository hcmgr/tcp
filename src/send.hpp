#pragma once
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>

#include <event2/event.h>

#include "buffer.hpp"
#include "define.hpp"
#include "cong.hpp"
#include "utils.hpp"

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
        uint16_t flags;
        uint64_t payload_pos;   // position in physical buffer of first byte
        uint64_t payload_len;
    };

    // un-ack'd / in-flight segments
    std::deque<segment> in_flight_segments_;

    std::unique_ptr<congestion_controller> cong_;

    uint16_t peer_recv_window_;

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

    enum class state {
        ESTABLISHED,
        FIN_PENDING,
        FIN_SENT,
        FINISHED
    };
    state state_;

public:
    send_stream(uint64_t capacity, send_segment_cb send_segment_cb);
    ~send_stream();

public:
    // user write new bytes to send
    int64_t write(uint64_t n, uint8_t *src_buffer);

    // peer ack'd our stream
    int64_t on_ack_recv(uint64_t acknum);

    // special sends that consume a seqnum (ack/rst don't consume a seqnum, so connection just sends them directly)
    int64_t send_syn();
    int64_t send_syn_ack();
    int64_t send_fin();

    uint64_t get_num_in_flight_bytes();
    uint64_t get_num_ready_bytes();
    uint64_t get_num_free_space_bytes();

    void set_peer_recv_window(uint16_t peer_recv_window) { peer_recv_window_ = peer_recv_window; }

    std::string to_string() { return ""; }

    void on_retransmission_timeout();

private:
    // send as many next-ready segments as our send window allows
    int64_t send_ready_bytes();

    // retransmit oldest un-ack'd segment - triggered by congestion event (rto or triple-dup-ack)
    int64_t retransmit_oldest_segment();

    // record a just-sent segment as in-flight for retransmission, arming the rto timer if needed
    int64_t buffer_segment_for_retransmission(const segment &seg);

    int64_t get_send_window() { return std::min(cong_->get_cwnd(), (int64_t)peer_recv_window_); }

    // increment circ-buffer position `pos` by `n`
    uint64_t inc(uint64_t pos, uint64_t n) const { return (pos + n) % buffer_->capacity(); }
};