#pragma once
#include <unordered_map>
#include <cstdint>
#include <thread>
#include <event2/event.h>

#include "connection.hpp"

//
// Static singleton manager of our connections - exists for life of process.
//
class connection_manager {
private:
    std::unordered_map<uint64_t, connection*> active_connections_;
    uint64_t connection_id_gen_;

    std::thread worker_;
    struct event_base *event_base_;

private:
    connection_manager()
        : connection_id_gen_(0)
    {
        event_base_ = event_base_new();
        if (!event_base_) {
            throw std::runtime_error("event_base null on creation");
        }

        auto worker_fn = [base = event_base_]() {
            event_base_loop(base, EVLOOP_NO_EXIT_ON_EMPTY);
        };
        worker_ = std::thread(worker_fn);
    }

    ~connection_manager() {}

public:
    static connection_manager &get_instance() {
        static connection_manager mgr;
        return mgr;
    }

public:
    // API

};