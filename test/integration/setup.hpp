#pragma once
#include <arpa/inet.h>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>

#include "../../src/define.hpp"
#include "../../src/utils.hpp"

struct segment {
    tcp_header hdr;
    std::vector<uint8_t> payload;
};

//
// Lightweight mock peer for the connection-under-test to send/recv to/from.
//
class mock_peer {
private:
    int fd_;
    uint16_t port_;
    uint16_t conn_port_;

public:
    mock_peer(uint16_t port, uint16_t conn_port, int timeout_ms = 1000)
        : port_(port), conn_port_(conn_port)
    {
        fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) {
            throw std::runtime_error("mock_peer socket()");
        }

        timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &local.sin_addr);
        if (bind(fd_, (sockaddr*)&local, sizeof(local)) < 0) {
            throw std::runtime_error("mock_peer bind()");
        }

        sockaddr_in remote{};
        remote.sin_family = AF_INET;
        remote.sin_port = htons(conn_port);
        inet_pton(AF_INET, "127.0.0.1", &remote.sin_addr);
        if (connect(fd_, (sockaddr*)&remote, sizeof(remote)) < 0) {
            throw std::runtime_error("mock_peer connect()");
        }
    }

    ~mock_peer() { ::close(fd_); }

    std::optional<segment> recv() {
        uint8_t buf[MSS + sizeof(tcp_header)];
        ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
        if (n < (ssize_t)sizeof(tcp_header)) {
            return std::nullopt;
        }
        segment seg;
        std::memcpy(&seg.hdr, buf, sizeof(tcp_header));
        seg.payload.assign(buf + sizeof(tcp_header), buf + n);
        return seg;
    }

    void send(uint32_t seqnum, uint32_t acknum, uint16_t flags, std::vector<uint8_t> payload = {}) {
        tcp_header hdr{};
        hdr.src_port = port_;
        hdr.dest_port = conn_port_;
        hdr.seqnum = seqnum;
        hdr.acknum = acknum;
        hdr.flags = flags;
        hdr.window = RECV_BUFFER_CAPACITY - 1;
        hdr.checksum = net::tcp_checksum_calc(hdr, payload.data(), payload.size());

        std::vector<uint8_t> buf(sizeof(tcp_header) + payload.size());
        std::memcpy(buf.data(), &hdr, sizeof(tcp_header));
        std::copy(payload.begin(), payload.end(), buf.begin() + sizeof(tcp_header));
        ::send(fd_, buf.data(), buf.size(), 0);
    }
};

//
// Our connection-under-test runs on its own thread, which this class manages.
// Tests post(fn) calls to the thread, e.g. open/read/write/close.
//
class connection_thread {
private:
    std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> tasks_;
    bool stop_ = false;
    std::thread worker_;

public:
    connection_thread() : worker_([this] { run(); }) {}

    ~connection_thread() {
        {
            std::lock_guard<std::mutex> lg(mtx_);
            stop_ = true;
        }
        cv_.notify_all();
        worker_.join();
    }

    template <typename F>
    std::future<std::invoke_result_t<F>> post(F fn) {
        auto task = std::make_shared<std::packaged_task<std::invoke_result_t<F>()>>(std::move(fn));
        auto fut = task->get_future();
        {
            std::lock_guard<std::mutex> lg(mtx_);
            tasks_.push_back([task] { (*task)(); });
        }
        cv_.notify_one();
        return fut;
    }

private:
    void run() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> ul(mtx_);
                cv_.wait(ul, [this] { return stop_ || !tasks_.empty(); });
                if (tasks_.empty()) {
                    return;
                }
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            task();
        }
    }
};
