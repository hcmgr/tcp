#pragma once
#include <event2/event.h>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <unistd.h>

#include "define.hpp"
#include "send.hpp"
#include "recv.hpp"
#include "utils.hpp"

struct addr_tuple {
    std::string src_ip_;
    std::string dest_ip_;
    uint32_t src_port_;
    uint32_t dest_port_;

    bool operator==(const addr_tuple &other) const {
        return src_ip_ == other.src_ip_ &&
               dest_ip_ == other.dest_ip_ &&
               src_port_ == other.src_port_ &&
               dest_port_ == other.dest_port_;
    }

    std::string to_string() const;
};

class connection {
private:
    uint64_t id_;

    addr_tuple addr_tuple_;

    tcp_state state_;

    int udp_socket_fd_;

    std::unique_ptr<send_stream> send_stream_;
    std::unique_ptr<recv_stream> recv_stream_;

    struct event *recv_segment_ev_;

    std::unique_ptr<timeout_handler> delayed_ack_timeout_;

    std::mutex pending_open_mtx_;
    std::condition_variable pending_open_cv_;

    std::mutex pending_read_mtx_;
    std::condition_variable pending_read_cv_;

    std::mutex pending_close_mtx_;
    std::condition_variable pending_close_cv_;

public:
    connection(uint64_t id, const addr_tuple &addr_tuple);

    ~connection();

public:
    //
    // User-facing open/read/write/close API.
    //
    int64_t open(const conn_type &conn_type);
    int64_t read(uint64_t n, uint8_t *dest_buffer);
    int64_t write(uint64_t n, uint8_t *src_buffer);
    int64_t shutdown();

public:
    //
    // Send segment to peer with given seqnum, flags, and optionally a payload.
    // Called by send_stream, as it decides when to send segments.
    //
    int64_t send_segment(uint64_t seqnum, uint16_t flags, uint8_t *payload_ptr, uint64_t payload_len);

    //
    // Callback from event loop on receipt of segment (i.e. bytes ready-to-read on udp socket).
    // Advance tcp state machine, hand off non-zero payload to recv_stream.
    //
    void on_recv_segment();

    //
    // Callback from event loop on trigger of delayed-ack timeout
    //
    void on_delayed_ack_timeout();

public:
    uint64_t get_id() { return id_; }
    addr_tuple get_addr_tuple() { return addr_tuple_; }
    tcp_state get_state() { return state_; }

    void destroy();

private:
    void reset();
    tcp_header make_header(uint32_t seqnum, uint16_t flags, uint8_t *payload_ptr, uint64_t payload_len);

    // header-only sends
    int64_t send_syn();
    int64_t send_syn_ack();
    int64_t send_fin();
    int64_t send_ack();
    int64_t send_rst();
};
