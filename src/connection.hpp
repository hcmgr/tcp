#pragma once
#include <event2/event.h>
#include <mutex>
#include <condition_variable>

#include "define.hpp"
#include "send.hpp"
#include "recv.hpp"
#include "utils.hpp"

enum class conn_type {
    CONNECT,
    LISTEN
};

class connection {
private:
    // 4-tuple, uniquely identifies connection - passed down by connection_manager
    std::string src_ip_;
    std::string dest_ip_;
    uint32_t src_port_;
    uint32_t dest_port_;

    // event_base of our worker/event loop - passed down by connection_manager
    struct event_base *event_base_;

    int udp_socket_fd_;

    send_stream *send_stream_;
    recv_stream *recv_stream_;

    tcp_state state_;

    std::mutex pending_open_mtx_;
    std::condition_variable pending_open_cv_;

    std::mutex pending_read_mtx_;
    std::condition_variable pending_read_cv_;

    std::mutex pending_close_mtx_;
    std::condition_variable pending_close_cv_;

public:
    connection(const std::string &src_ip,
               const std::string &dest_ip,
               uint32_t src_port,
               uint32_t dest_port,
               struct event_base *event_base,
               conn_type ct);

    ~connection();

public:
    int64_t open();

    int64_t read(uint64_t n, uint8_t *dest_buffer);

    int64_t write(uint64_t n, uint8_t *src_buffer);

    int64_t close();

public:
    void on_recv_segment();

private:
    void reset();
    void destroy();
};
