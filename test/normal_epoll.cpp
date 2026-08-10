#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <iostream>
#include <format>
#include <stdlib.h>
#include <vector>

#define MAX_CONNS 4096
#define SERVER_PORT 10002

//
// Server event-loop:
//      - continuously waits for r/w socket events across all client sockets
//      - on read() avail
//          - reads num sent by client, increments, writes num back
//
void event_loop(int epfd_) {
    int epfd = epfd_;
    struct epoll_event events[MAX_CONNS];

    while (1) {
        int n = epoll_wait(epfd, events, MAX_CONNS, -1);
        for (int i = 0; i < n; i++) {
            struct epoll_event event = events[i];
            int fd = event.data.fd;
            if (!(event.events & EPOLLIN) && !(event.events & EPOLLOUT)) {
                throw std::runtime_error("bad event - neither EPOLLIN nor EPOLLOUT");
            }

            if (event.events & EPOLLIN) {
                char buff[1024];
                int m = read(fd, buff, 1024);
                if (m < 0) {
                    throw std::runtime_error(std::format("bad read - {}", errno));
                }
                if (m == 0) {
                    continue;
                }

                int num = atoi(buff);
                if (num == 0) {
                    throw std::runtime_error(std::format("bad atoi() - {}", errno));
                }

                //
                // write num+1 back to client
                //
                num++;
                std::string s = std::to_string(num);
                if (s.size() == 0) {
                    throw std::runtime_error("bad str conversion");
                }
                m = write(fd, s.c_str(), s.size() + 1);
                if (m < 0) {
                    throw std::runtime_error(std::format("bad write - {}", errno));
                }
            }
        }
    }
}

//
// Server:
//      - listens on port SERVER_PORT for incoming client connections
//      - on client connect, dispatches r/w socket event to the epoll event-loop
//
int run_server() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        throw std::runtime_error("bad socket()");
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(SERVER_PORT);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        throw std::runtime_error(std::format("bad bind() - {}", errno));
    }
    if (listen(listen_fd, MAX_CONNS) < 0) {
        throw std::runtime_error(std::format("bad listen() - {}", errno));
    }

    // create epoll instance and dispatch an event loop thread
    int epfd = epoll_create(1);
    std::thread event_loop_thread(event_loop, epfd);
    event_loop_thread.detach();

    while (1) {
        int peer_fd = accept(listen_fd, nullptr, nullptr);
        if (peer_fd < 0) {
            throw std::runtime_error(std::format("bad accept() - {}", errno));
        }

        // queue event to trigger on peer_fd read()/write() available
        struct epoll_event event;
        event.events = EPOLLIN;
        event.data.fd = peer_fd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, peer_fd, &event);
    }
}

//
// Client:
//      - connects to server on port SERVER_PORT
//      - continuously:
//          - write()'s num to server
//          - read()'s new_num from server (expects new_num == num + 1, i.e. server incremented it)
//
int run_client(int init_num) {
    // connect to server
    int connect_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(SERVER_PORT);
    if (connect(connect_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        throw std::runtime_error(std::format("bad connect() - {}", errno));
    }

    int num = init_num;
    std::vector<int> nums_received;

    while (num < init_num + 500) {
        // write current num to server
        std::string s = std::to_string(num);
        write(connect_fd, s.c_str(), s.size() + 1);

        // read new_num from server - expect new_num == num + 1
        char buff[1024];
        int m = read(connect_fd, buff, 1024);
        if (m == 0) {
            throw std::runtime_error(std::format("bad read() - {}", errno));
        }
        int new_num = atoi(buff);
        if (new_num == 0) {
            throw std::runtime_error(std::format("bad atoi() - {}", errno));
        }
        if (new_num != num + 1) {
            throw std::runtime_error(std::format("server didn't increment num we sent - expected={}, actual={}", num, new_num));
        }
        nums_received.push_back(new_num);

        num = new_num;
    }

    close(connect_fd);

    std::cout << "init_num: " << init_num << "\n";
    std::cout << "nums received: ";
    for (auto &el : nums_received) {
        std::cout << el << " ";
    }
    std::cout << "\n";

    return 0;
}

int main() {
    // start server
    std::thread server(run_server);

    sleep(1);
    
    // start client
    int n = 50;
    for (int i = 0; i < n; i++) {
        int init_num = i + 1;
        std::thread client(run_client, init_num);
        client.detach();
    }

    server.join();
    return 0;
}