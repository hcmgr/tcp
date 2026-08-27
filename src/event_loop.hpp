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

struct timeout_handler {
    bool active;
    struct event *ev;
    uint32_t timeout_ms;

    timeout_handler(uint32_t timeout_ms, event_callback_fn cb, void *arg)
        : active(false), ev(nullptr), timeout_ms(timeout_ms)
    {
        ev = event_new(manager::get_instance().get_event_base(), -1, EV_TIMEOUT, cb, arg);
        if (ev == nullptr) {
            clear();
            return;
        }

        active = true;
    }

    ~timeout_handler() { clear(); }

    int64_t add() {
        if (active) {
            Log(level::ERROR, "timeout_handler add() whilst active");
            return -1;
        }

        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        int res = event_add(ev, &tv);
        if (res < 0) {
            Log(level::ERROR, std::format("timeout_handler add() failed - {}", strerror(errno)));
            return -1;
        }

        active = true;
        return 0;
    }

    void clear() {
        event_free(ev);
        ev = nullptr;
        active = false;
    }
};