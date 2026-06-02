#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <random>
#include <thread>
#include <format>
#include <sys/ioctl.h>

#include <event2/event.h>

#include "engine.hpp"

Connection::Connection() {}

Engine::Engine()
    : connectionIdGen(0) 
{
    Log(INFO, "initialising engine");
    eventBase = event_base_new();
    thread = std::thread(&Engine::eventLoop, this);
}

Engine::~Engine() {
    event_base_loopexit(eventBase, nullptr);
    if (thread.joinable()) {
        thread.join();
    }
    event_base_free(eventBase);
}

void Engine::eventLoop() {
    event_base_loop(eventBase, EVLOOP_NO_EXIT_ON_EMPTY);
}

Engine& Engine::getInstance() {
    static Engine engine;
    return engine;
}

//////////////////////////////////////////////////////////////////
// open/read/write/close API - called by TcpConn for given `cId`.
//
// These calls occur on normal user threads.
//////////////////////////////////////////////////////////////////

//
// Blocking call. Returns execution once connection fully established with peer:
//      - create connection with unique connectionId
//      - initialise udp socket and rest of state
//      - note that we wait until in ESTABLISHED state for both LISTEN and CONNECT connType's
//
int64_t Engine::open(const std::string srcIp, 
                     int srcPort, 
                     const std::string destIp,
                     int destPort,
                     ConnType connType) 
{
    int cId = connectionIdGen;

    //
    // create socket
    //
    int udpSocketFd = createUdpSocket(srcIp, srcPort, destIp, destPort);
    if (udpSocketFd < 0) {
        Log(ERROR, "error opening udp socket");
        return -1;
    }

    //
    // setup Connection object
    //
    Connection* conn = new Connection();
    conn->id = cId;

    conn->srcIp = srcIp;
    conn->srcPort = srcPort;
    conn->destIp = destIp;
    conn->destPort = destPort;

    conn->udpSocketFd = udpSocketFd;

    if (connType == ConnType::LISTEN) {
        conn->state = State::LISTEN;
    } else if (connType == ConnType::CONNECT) {
        conn->state = State::CLOSED;
    } else {
        Log(ERROR, std::format("incorrect connType: {}", toString(connType)));
        return -1;
    }

    //
    // register the 'segment arrive' event
    //
    struct event *segmentArrive = event_new(eventBase, udpSocketFd, EV_READ|EV_PERSIST, libeventSegmentArrive, (void*)conn);
    event_add(segmentArrive, NULL);

    connectionIdGen++;
    return cId;
}

void Engine::read(int64_t cId, int n, std::vector<uint8_t> &buffer) {
    auto it = openConnections.find(cId);
    if (it == openConnections.end()) {
        Log(ERROR, std::format("couldn't find connection: {}", cId));
        return;
    }
    Connection* conn = it->second;
}

void Engine::write(int64_t cId, int n, std::vector<uint8_t> &buffer) {
    auto it = openConnections.find(cId);
    if (it == openConnections.end()) {
        Log(ERROR, std::format("couldn't find connection: {}", cId));
        return;
    }
    Connection* conn = it->second;
}

void Engine::close(int64_t cId) {

}

//////////////////////////////////////////////////////////////////
// Event handlers
//////////////////////////////////////////////////////////////////

void Engine::handleSegmentArrive(Connection *conn) {
    Log(INFO, std::format("segment arrive - state={}", toString(conn->state)));

    // validate connection is valid and open
    if (!conn) {
        Log(ERROR, "conn null");
        return;
    }
    auto it = openConnections.find(conn->id);
    if (it == openConnections.end() || it->second != conn) {
        Log(ERROR, "engine has no matching open connection");
        return;
    }

    //
    // Read segment into intermediate buffer
    //
    int maxBytesToRead = sizeof(Header) + MSS;
    if (conn->recvStream.freeSpace() < maxBytesToRead) {
        Log(ERROR, "insufficient free space in recvBuffer for header + segment - waiting until space free'd");
        return;
    }

    uint8_t *buffer = (uint8_t*)malloc(maxBytesToRead);
    int bytesRead = recv(conn->udpSocketFd, buffer, maxBytesToRead, 0);
    if (bytesRead == -1) {
        Log(ERROR, std::format("error on recv() - {}", strerror(errno)));
        return;
    }
    Log(INFO, std::format("segment read - {} bytes total (header + payload)", bytesRead));

    //
    // Process TCP header
    //
    struct Header hdr = (struct Header)*buffer;

    switch (conn->state) {
        case State::LISTEN: {
            if (hdr.FIN || hdr.RST) {
                Log(ERROR, std::format("state=LISTEN - FIN/RST set - invalid - sending RST to teardown connection - {}", hdr.toString()));
                return;
            }

            if (hdr.SYN && !hdr.ACK) {
                //
                // SYN received
                //
                Log(INFO, "state=LISTEN - SYN received");

                // initialise recvStream (IRS == hdr.seqNum)
                conn->recvStream.init(hdr.seqNum);

                // initialise sendStream
                conn->sendStream.init();

                // send syn-ACK
                Header hdr;
                hdr.srcPort = conn->srcPort;
                hdr.destPort = conn->destPort;
                hdr.SYN = 1;
                hdr.seqNum = conn->sendStream.SND_ISS;
                hdr.ACK = 1;
                hdr.ackNum = conn->recvStream.RCV_NXT;
                hdr.window = conn->recvStream.RCV_WND;

                conn->state = State::SYN_RECEIVED;
            } else {
                // all other combos are invalid
                Log(ERROR, std::format("state=LISTEN, didn't receive SYN - invalid - {}", hdr.toString()));
                return;
            }
        } break;
        case State::SYN_SENT: {
            if (hdr.FIN || hdr.RST) {
                Log(ERROR, std::format("state=SYN_SENT - FIN/RST set - invalid - sending RST to teardown connection - {}", hdr.toString()));
                return;
            }

            if (hdr.SYN && hdr.ACK) {
                //
                // SYN-ACK received
                //
                Log(INFO, "state=SYN_SENT - SYN-ACK received");

                // initialise recvStream (IRS=hdr.seqNum)
                conn->recvStream.init(hdr.seqNum);

                // verify the ACK of our ISS
                // TODO - need to further verify that ackNum == SND.ISS+1 ?
                conn->sendStream.onAck(hdr.ackNum);

                // ACK peer's SYN
                // TODO - ACK peer's SYN

                conn->state = State::ESTABLISHED;
            } else {
                // all other combos are invalid
                Log(ERROR, std::format("state=SYN-SENT, didn't receive SYN-ACK - invalid - {}", hdr.toString()));
                return;
            }
        } break;
        case State::SYN_RECEIVED: {
            if (hdr.FIN || hdr.RST) {
                Log(ERROR, std::format("state=SYN_SENT - FIN/RST set - invalid - sending RST to teardown connection - {}", hdr.toString()));
                return;
            }

            if (hdr.ACK && !hdr.SYN) {
                //
                // ACK received
                //
                Log(INFO, "state=SYN_RECEIVED - ACK received");

                // verify the ACK of our SND_ISS
                // TODO - need to further verify that ackNum == SND.ISS+1 ?
                conn->sendStream.onAck(hdr.ackNum);

                conn->state = State::ESTABLISHED;
            } else {
                // all other combos are invalid
                Log(ERROR, std::format("state=SYN-RECEIVED, didn't receive SYN-ACK - invalid - {}", hdr.toString()));
                return;
            }
        } break;
        case State::ESTABLISHED: {

        } break;
    }
}

//////////////////////////////////////////////////////////////////
// Rest of Engine methods
//////////////////////////////////////////////////////////////////

int Engine::createUdpSocket(const std::string& srcIp,
                            int srcPort,
                            const std::string& dstIp,
                            int dstPort) 
{
    // create socket
    int fd = socket(AF_INET, SOCK_DGRAM, 17);
    if (fd == -1) {
        perror("socket");
        Log(ERROR, std::format("socket() - {}", strerror(errno)));
        return -1;
    }

    //
    // bind local/source address
    //
    sockaddr_in localAddr{};
    localAddr.sin_family = AF_INET;
    localAddr.sin_port = htons(srcPort);
    if (inet_pton(AF_INET, srcIp.c_str(), &localAddr.sin_addr) != 1) {
        Log(ERROR, std::format("inet_pton() localAddr - {}", strerror(errno)));
        close(fd);
        return -1;
    }
    if (bind(fd, (sockaddr*)&localAddr, sizeof(localAddr)) == -1) {
        Log(ERROR, std::format("bind() localAddr - {}", strerror(errno)));
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
        Log(ERROR, std::format("inet_pton() peerAddr - {}", strerror(errno)));
        close(fd);
        return -1;
    }
    if (connect(fd, (sockaddr*)&peerAddr, sizeof(peerAddr)) == -1) {
        Log(ERROR, std::format("connect() peerAddr - {}", strerror(errno)));
        close(fd);
        return -1;
    }

    return fd;
}

//////////////////////////////////////////////////////////////////
// Libevent callbacks
//////////////////////////////////////////////////////////////////

void libeventSegmentArrive(evutil_socket_t fd, short events, void* arg) {
    if (!(events & EV_READ)) {
        Log(ERROR, "EV_READ not triggered");
        return;
    }
    Connection *conn = (Connection*)arg;
    if (conn == nullptr) {
        Log(ERROR, "conn null");
        return;
    }
    if (conn->udpSocketFd != fd) {
        Log(ERROR, "udp socket doesn't match event fd");
        return;
    }
    Engine::getInstance().handleSegmentArrive(conn);
}