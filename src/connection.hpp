#pragma once
#include <event2/event.h>
#include <mutex>
#include <condition_variable>

#include "define.hpp"
#include "send.hpp"
#include "recv.hpp"
#include "utils.hpp"

struct addr_tuple {
    std::string src_ip_;
    std::string dest_ip_;
    uint32_t src_port_;
    uint32_t dest_port_;
};

enum class conn_type {
    CONNECT,
    LISTEN
};

class connection {
private:
    // monotonically increasing id given by manager
    uint64_t id_;

    addr_tuple addr_tuple_;
    conn_type conn_type_;

    tcp_state state_;

    int udp_socket_fd_;

    send_stream *send_stream_;
    recv_stream *recv_stream_;

    timeout_handler *delayed_ack_timeout_;

    std::mutex pending_open_mtx_;
    std::condition_variable pending_open_cv_;

    std::mutex pending_read_mtx_;
    std::condition_variable pending_read_cv_;

    std::mutex pending_close_mtx_;
    std::condition_variable pending_close_cv_;

public:
    connection(const addr_tuple &addr_tuple,
               const conn_type &ct);

    ~connection();

public:
    //
    // User-facing open/read/write/close API.
    //
    int64_t open();
    int64_t read(uint64_t n, uint8_t *dest_buffer);
    int64_t write(uint64_t n, uint8_t *src_buffer);
    int64_t close();

public:
    //
    // Send segment to peer with given seqnum, flags, and optionally a payload.
    // Called by: 
    //      a) send_stream, to send payload segments, or 
    //      b) connection itself, to send header-only segments
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
    tcp_state get_state() { return state_; }
    void destroy();

private:
    void reset();
    tcp_header make_header(uint32_t seqnum, uint16_t flags, uint8_t *payload_ptr, uint64_t payload_len);

    // header-only sends
    // note: 'fin' piggybacks last segment, so we don't send it header-only
    int64_t send_syn();
    int64_t send_syn_ack();
    int64_t send_ack();
    int64_t send_rst();
};
