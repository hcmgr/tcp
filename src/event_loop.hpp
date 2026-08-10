#pragma once
#include "connection.hpp"

inline void libevent_on_recv_segment(evutil_socket_t fd, short events, void* arg) {
    if (!(events & EV_READ)) {
        Log(level::ERROR, "EV_READ not triggered");
        return;
    }
    connection *conn = (connection*)arg;
    if (conn == nullptr) {
        Log(level::ERROR, "connection null");
        return;
    }
    conn->on_recv_segment();
}

inline void libevent_on_rto(evutil_socket_t fd, short events, void* arg) {
    if (!(events & EV_TIMEOUT)) {
        Log(level::ERROR, "EV_TIMEOUT not triggered");
        return;
    }
}