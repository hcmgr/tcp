#pragma once
#include "connection.hpp"
#include "send.hpp"
#include "utils.hpp"
#include "manager.hpp"

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

inline void libevent_on_delayed_ack_timeout(evutil_socket_t fd, short events, void* arg) {
    if (!(events & EV_TIMEOUT)) {
        Log(level::ERROR, "EV_TIMEOUT not triggered");
        return;
    }

    connection *conn = (connection*)arg;
    if (conn == nullptr) {
        Log(level::ERROR, "connection null");
        return;
    }

    conn->on_delayed_ack_timeout();
}

inline void libevent_on_time_wait_timeout(evutil_socket_t fd, short events, void* arg) {
    if (!(events & EV_TIMEOUT)) {
        Log(level::ERROR, "EV_TIMEOUT not triggered");
        return;
    }

    connection *conn = (connection*)arg;
    if (conn == nullptr) {
        Log(level::ERROR, "connection null");
        return;
    }

    conn->on();
}

inline void libevent_on_retransmission_timeout(evutil_socket_t fd, short events, void* arg) {
    if (!(events & EV_TIMEOUT)) {
        Log(level::ERROR, "EV_TIMEOUT not triggered");
        return;
    }

    send_stream *ss = (send_stream*)arg;
    if (ss == nullptr) {
        Log(level::ERROR, "send_stream null");
        return;
    }

    ss->on_retransmission_timeout();
}