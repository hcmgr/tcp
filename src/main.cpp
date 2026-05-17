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

/**
 * creates udp socket on localhost:8000
 * sits in a loop of:
 *      - sends one packet
 *      - sleeps for random time between 1-10ms
 */
void producer() {
    std::string sourceIp = "127.0.0.1"; // localhost
    int sourcePort = 8000;
    std::string destIp = "127.0.0.1";
    int destPort = 8001;
    int udpSocketFd = createUdpSocket(sourceIp, sourcePort, destIp, destPort);
    if (udpSocketFd == -1) {
        return;
    }

    int packetCnt = 0;
    while (true) {
        // send one packet
        std::string packetData = "packet:" + std::to_string(packetCnt);
        ssize_t sent = send(udpSocketFd, (void*)packetData.c_str(), packetData.length(), 0);
        if (sent == -1) {
            perror("error sending udp data");
        }
        packetCnt++;

        // sleep for random interval in ms
        std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<> dist(10, 50);
        int interval = dist(generator);
        std::cout << "bytes sent - " << sent << " - " << packetData << " - sleeping for - " << interval << "ms" << "\n";
        usleep(interval * 1000);
    }
}

/**
 * creates udp socket on localhost:8000
 * sits in loop of:
 *      - blocking recvfrom() call
 *      - echoes any bytes avail
 *      - sleeps again
 */
void consumer() {
    std::string sourceIp = "127.0.0.1"; // localhost
    int sourcePort = 8001;
    std::string destIp = "127.0.0.1";
    int destPort = 8000;
    int udpSocketFd = createUdpSocket(sourceIp, sourcePort, destIp, destPort);
    if (udpSocketFd == -1) {
        return;
    }

    std::string packetData;
    packetData.resize(1024);
    while (true) {
        // receive one packet
        int received = recv(udpSocketFd, packetData.data(), 1024, 0);
        if (received == -1) {
            perror("error receiving udp data");
        }
        std::cout << "bytes received - " << received << " - " << packetData << "\n";
    }
}

void test_udp_spsc() {
    std::thread p(producer);
    std::thread c(consumer);
    p.join();
    c.join();
}

void test_event() {
    struct event_base *base;
    base = event_base_new();

    event_base_dispatch(base);
}

int main() {
    test_event();
    return 0;
}
