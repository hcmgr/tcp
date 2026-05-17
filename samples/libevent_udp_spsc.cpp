#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <random>
#include <thread>

#include <event2/event.h>

int createUdpSocket(const std::string& srcIp,
                    int srcPort,
                    const std::string& dstIp,
                    int dstPort) 
{
    // create socket
    int fd = socket(AF_INET, SOCK_DGRAM, 17);
    if (fd == -1) {
        perror("socket");
        return -1;
    }

    //
    // bind local/source address
    //
    sockaddr_in localAddr{};
    localAddr.sin_family = AF_INET;
    localAddr.sin_port = htons(srcPort);
    if (inet_pton(AF_INET, srcIp.c_str(), &localAddr.sin_addr) != 1) {
        perror("inet_pton local");
        close(fd);
        return -1;
    }
    if (bind(fd, (sockaddr*)&localAddr, sizeof(localAddr)) == -1) {
        perror("bind");
        close(fd);
        return -1;
    }

    //
    // connect peer/dest address
    //
    sockaddr_in peerAddr{};
    peerAddr.sin_family = AF_INET;
    peerAddr.sin_port = htons(dstPort);
    if (inet_pton(AF_INET, dstIp.c_str(), &peerAddr.sin_addr) != 1) {
        perror("inet_pton peer");
        close(fd);
        return -1;
    }
    if (connect(fd, (sockaddr*)&peerAddr, sizeof(peerAddr)) == -1) {
        perror("connect");
        close(fd);
        return -1;
    }

    return fd;
}

struct produce_args {
    int udpSocketFd;
    int packetCnt;
    struct event_base *base;
};

void do_produce(evutil_socket_t fd, short events, void *arg) {
    struct produce_args *pa = (struct produce_args*)arg;

    // send one packet
    std::string packetData = "packet:" + std::to_string(pa->packetCnt);
    ssize_t sent = send(pa->udpSocketFd, (void*)packetData.c_str(), packetData.length(), 0);
    if (sent == -1) {
        perror("error sending udp data");
        return;
    }
    pa->packetCnt++;

    // generate random timeout
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<> dist(100, 500);
    int interval = dist(generator);

    // schedule new produce event after random timeout
    struct event *produceEvent = event_new(pa->base, -1, EV_TIMEOUT, do_produce, (void*)pa);
    struct timeval tp{};
    tp.tv_usec = interval * 1000;
    event_add(produceEvent, &tp);
}

struct consume_args {
    int udpSocketFd;
    struct event_base* base;
};

void do_consume(evutil_socket_t fd, short events, void *arg) {
    struct consume_args *ca = (struct consume_args*)arg;

    // receive one packet
    std::string packetData;
    packetData.resize(1024);
    int received = recv(ca->udpSocketFd, packetData.data(), 1024, 0);
    if (received == -1) {
        perror("error receiving udp data");
        return;
    }
    if (received == 0) {
        perror("no data available");
        return;
    }
    std::cout << "received - " << packetData << "\n";
}

void init() {
    struct event_base *base;
    struct event *produceEvent;
    struct event *consumeEvent;
    struct produce_args *pa;
    struct consume_args *ca;

    base = event_base_new();

    pa = (struct produce_args*)malloc(sizeof(struct produce_args));
    pa->udpSocketFd = createUdpSocket("127.0.0.1", 8000, "127.0.0.1", 8001);
    pa->packetCnt = 0;
    pa->base = base;

    ca = (struct consume_args*)malloc(sizeof(struct consume_args));
    ca->udpSocketFd = createUdpSocket("127.0.0.1", 8001, "127.0.0.1", 8000);
    ca->base = base;

    produceEvent = event_new(base, -1, EV_TIMEOUT, do_produce, (void*)pa);
    consumeEvent = event_new(base, ca->udpSocketFd, EV_READ|EV_PERSIST, do_consume, (void*)ca);

    struct timeval now{};
    event_add(produceEvent, &now); // run first produce immediately, rest after a random timeout
    event_add(consumeEvent, NULL);

    event_base_dispatch(base);
}

int main() {
    init();
    return 0;
}
