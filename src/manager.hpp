#pragma once
#include <unordered_map>
#include <cstdint>
#include <thread>
#include <event2/event.h>
#include <memory>

#include "connection.hpp"

//
// Static singleton manager of our connections - exists for life of process.
//
class manager {
private:
    std::unordered_map<uint64_t, std::shared_ptr<connection>> active_connections_;
    uint64_t connection_id_gen_;

    std::thread worker_;
    struct event_base *event_base_;

private:
    manager()
        : connection_id_gen_(0)
    {
        start_event_loop();
    }

    ~manager() {
        destroy();
    }

public:
    static manager &get_instance() {
        static manager mgr;
        return mgr;
    }

    struct event_base* get_event_base() { return event_base_; }

    std::shared_ptr<connection> add_connection() {

    }

    void remove_connection(uint64_t id) {
        auto it = active_connections_.find(id);
        if (it == active_connections_.end()) {
            return;
        }
        it->second->destroy();
        active_connections_.erase(it);
    }

private:
    void start_event_loop() {
        event_base_ = event_base_new();
        if (!event_base_) {
            throw std::runtime_error("event_base null on creation");
        }

        auto worker_fn = [base = event_base_]() {
            event_base_loop(base, EVLOOP_NO_EXIT_ON_EMPTY);
        };
        worker_ = std::thread(worker_fn);
    }

    void destroy() {
        //
        // Clear active connections - RAII destroy()'s any open connections. 
        // Means we are aggressively closing our side of the connection. 
        // Given we're at the end of our process here, not much else we can do.
        //
        active_connections_.clear();

        // stop and free event loop
        event_base_free(event_base_);
        event_base_ = nullptr;

        // stop worker thread
        worker_.join();
    }
};