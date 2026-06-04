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

Connection::Connection(
    int64_t id,
    std::string srcIp,
    int srcPort,
    std::string destIp,
    int destPort,
    int udpSocketFd,
    ConnType connType
) : 
    id(id),
    srcIp(srcIp), 
    srcPort(srcPort), 
    destIp(destIp), 
    destPort(destPort), 
    udpSocketFd(udpSocketFd),
    sendStream(this),
    recvStream()
{
    if (connType == ConnType::LISTEN) {
        state = State::LISTEN;
    } else if (connType == ConnType::CONNECT) {
        state = State::CLOSED;
    } else {
        Log(ERROR, std::format("incorrect connType: {} - defaulting to closed", toString(connType)));
        state = State::BAD;
        return;
    }

    sendSegmentBufferSize = sizeof(Header) + MSS;
    sendSegmentBuffer = (uint8_t*)malloc(sendSegmentBufferSize);
    recvSegmentBufferSize = sizeof(Header) + MSS;
    recvSegmentBuffer = (uint8_t*)malloc(recvSegmentBufferSize);
    if (sendSegmentBuffer == nullptr || recvSegmentBuffer == nullptr) {
        Log(ERROR, std::format("issue allocating send/recv segment buffer - {} {}", sendSegmentBuffer, recvSegmentBuffer));
        state = State::BAD;
        return;
    }
}

Connection::~Connection() {
    free(sendSegmentBuffer);
    free(recvSegmentBuffer);
    state = State::BAD;
}

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

int64_t Engine::open(const std::string srcIp, 
                     int srcPort, 
                     const std::string destIp,
                     int destPort,
                     ConnType connType) 
{
    int cId = connectionIdGen;

    // create socket
    int udpSocketFd = createUdpSocket(srcIp, srcPort, destIp, destPort);
    if (udpSocketFd < 0) {
        Log(ERROR, "error opening udp socket");
        return -1;
    }

    // setup Connection object
    Connection* conn = new Connection(cId, srcIp, srcPort, destIp, destPort, udpSocketFd, connType);
    if (conn->state == State::BAD) {
        Log(ERROR, "error initialising Connection object");
        return -1;
    }

    // register the 'recvSegment' event
    struct event *recvSegment = event_new(eventBase, udpSocketFd, EV_READ|EV_PERSIST, libeventRecvSegment, (void*)conn);
    event_add(recvSegment, NULL);

    if (conn->state == State::CLOSED) {
        // send initial SYN
        bool res = sendHandshakeSyn(conn);
        if (!res) {
            reset(conn);
            return -1;
        }
        conn->state = State::SYN_SENT;
    } else if (conn->state == State::LISTEN) {
        // wait for initial SYN - do nothing
    } else {
        // invalid state
        reset(conn);
        return -1;
    }

    if (openConnections.count(cId) != 0) {
        Log(ERROR, std::format("connection already exists with id={}", cId));
        return -1;
    }
    openConnections[cId] = conn;

    //
    // put open() call to sleep, waiting on connection state == ESTABLISHED
    //

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

void Engine::recvSegment(Connection *conn) {
    Log(INFO, std::format("segment arrive - state={}", toString(conn->state)));

    // verify connection is valid and open
    if (!conn) {
        Log(ERROR, "conn null");
        return;
    }
    auto it = openConnections.find(conn->id);
    if (it == openConnections.end() || it->second != conn) {
        Log(ERROR, "engine has no matching open connection");
        return;
    }

    // read segment (header + payload)
    int bytesRead = recv(conn->udpSocketFd, conn->recvSegmentBuffer, conn->recvSegmentBufferSize, 0);
    if (bytesRead == -1) {
        Log(ERROR, std::format("error on recv() - {}", strerror(errno)));
        return;
    }
    Log(INFO, std::format("segment read - {} bytes total (header + payload)", bytesRead));

    // process header
    struct Header hdr = (struct Header)*conn->recvSegmentBuffer;
    if (!verifyReceivedHeader(conn, hdr)) {
        sendRst(conn);
        conn->state = State::CLOSED;
        return;
    }

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

                // initialise recvStream with peer's ISS
                conn->recvStream.init(hdr.seqNum);

                // initialise sendStream
                conn->sendStream.init(hdr.window);

                // send SYN-ACK
                bool res = sendHandshakeSynAck(conn);
                if (!res) {
                    sendRst(conn);
                    conn->state = State::CLOSED;
                    return;
                }

                conn->state = State::SYN_RECEIVED;
            } else {
                Log(ERROR, std::format("state=LISTEN, didn't receive SYN - invalid - {}", hdr.toString()));
                return;
            }
        } break;
        case State::SYN_SENT: {
            if (hdr.FIN || hdr.RST) {
                Log(ERROR, std::format("state=SYN_SENT - FIN/RST set - invalid - sending RST"));
                return;
            }

            if (hdr.SYN && hdr.ACK) {
                //
                // SYN-ACK received
                //
                Log(INFO, "state=SYN_SENT - SYN-ACK received");

                // initialise recvStream with peer's ISS
                conn->recvStream.init(hdr.seqNum);

                // verify peer's ACK of our ISS
                bool res = conn->sendStream.onHandshakeAckRecv(hdr.ackNum);
                if (!res) {
                    sendRst(conn);
                    conn->state = State::CLOSED;
                    return;
                }

                // send ACK
                sendHandshakeAck(conn);

                conn->state = State::ESTABLISHED;
            } else {
                // TODO - send RST
                Log(ERROR, std::format("state=SYN-SENT, didn't receive SYN-ACK - invalid - sending RST - {}", hdr.toString()));
                return;
            }
        } break;
        case State::SYN_RECEIVED: {
            if (hdr.FIN || hdr.RST) {
                return;
            }

            if (hdr.ACK && !hdr.SYN) {
                //
                // ACK received
                //
                Log(INFO, "state=SYN_RECEIVED - ACK received");

                // verify peer's ACK of our ISS
                bool res = conn->sendStream.onHandshakeAckRecv(hdr.ackNum);
                if (!res) {
                    reset(conn);
                    return;
                }

                conn->state = State::ESTABLISHED;
            } else {
                Log(ERROR, std::format("state=SYN-RECEIVED, didn't receive ACK - invalid - sending RST - {}", hdr.toString()));
                return;
            }
        } break;
        case State::ESTABLISHED: {

        } break;
    }
}

int64_t Engine::sendSegment(Connection *conn, int64_t n, uint8_t *payloadBuffer) {
    if (n > MSS) {
        Log(ERROR, std::format("cannot send payload size > 1 MSS - {}", n));
        return -1;
    }

    Header hdr;
    hdr.srcPort = conn->srcPort;
    hdr.destPort = conn->destPort;
    hdr.seqNum = conn->sendStream.getNxt();
    hdr.ACK = 1;
    hdr.ackNum = conn->recvStream.getNxt();
    hdr.window = conn->recvStream.getWnd();
    hdr.setChecksum();

    std::memcpy(conn->sendSegmentBuffer, &hdr, sizeof(hdr));
    std::memcpy(conn->sendSegmentBuffer + sizeof(hdr), payloadBuffer, n);

    int bytesToSend = sizeof(hdr) + n;
    int bytesSent = send(conn->udpSocketFd, conn->sendSegmentBuffer, bytesToSend, 0);
    if (bytesSent == -1) {
        Log(ERROR, std::format("error on send() - {}", strerror(errno)));
        return -1;
    }
    return bytesSent;
}

int64_t Engine::sendSegmentHeaderOnly(Connection *conn, Header &hdr) {
    std::memcpy(conn->sendSegmentBuffer, &hdr, sizeof(hdr));

    int bytesSent = send(conn->udpSocketFd, conn->sendSegmentBuffer, sizeof(hdr), 0);
    if (bytesSent == -1) {
        Log(ERROR, std::format("error on send() - {}", strerror(errno)));
        return -1;
    }
    if (bytesSent != sizeof(hdr)) {
        Log(ERROR, std::format("did not write full header"));
        return -1;
    }
    return bytesSent;
}

bool Engine::sendHandshakeSyn(Connection *conn) {
    Header hdr;
    hdr.srcPort = conn->srcPort;
    hdr.destPort = conn->destPort;
    hdr.seqNum = conn->sendStream.getNxt(); // == iss
    hdr.window = conn->recvStream.getWnd();
    hdr.setChecksum();

    int64_t res = sendSegmentHeaderOnly(conn, hdr);
    if (res == -1) {
        return false;
    }

    bool res = conn->sendStream.onHandshakeSynSend();
    if (!res) {
        return false;
    }

    return true;
}

bool Engine::sendHandshakeSynAck(Connection *conn) {
    Header hdr;
    hdr.srcPort = conn->srcPort;
    hdr.destPort = conn->destPort;
    hdr.SYN = 1;
    hdr.seqNum = conn->sendStream.getNxt(); // == iss
    hdr.ACK = 1;
    hdr.ackNum = conn->recvStream.getNxt();
    hdr.window = conn->recvStream.getWnd();
    hdr.setChecksum();

    int64_t res = sendSegmentHeaderOnly(conn, hdr);
    if (res == -1) {
        return false;
    }

    bool res = conn->sendStream.onHandshakeSynSend();
    if (!res) {
        return false;
    }

    return true;
}

bool Engine::sendHandshakeAck(Connection *conn) {
    Header hdr;
    hdr.srcPort = conn->srcPort;
    hdr.destPort = conn->destPort;
    hdr.seqNum = conn->sendStream.getNxt();
    hdr.ACK = 1;
    hdr.ackNum = conn->recvStream.getNxt();
    hdr.window = conn->recvStream.getWnd();
    hdr.setChecksum();

    int64_t res = sendSegmentHeaderOnly(conn, hdr);
    if (res == -1) {
        return false;
    }

    return true;
}

bool Engine::sendRst(Connection *conn) {

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

bool Engine::verifyReceivedHeader(Connection *conn, const Header &hdr) {
    if (!(hdr.srcPort == conn->destPort && hdr.destPort == conn->srcPort)) {
        Log(ERROR, std::format("bad header - ports don't match - {}", hdr.toString()));
        return false;
    }
    if (!hdr.verifyChecksum()) {
        Log(ERROR, std::format("bad header - invalid checksum - {}", hdr.toString()));
        return false;
    }
    return true;
}

//////////////////////////////////////////////////////////////////
// Libevent callbacks
//////////////////////////////////////////////////////////////////

void libeventRecvSegment(evutil_socket_t fd, short events, void* arg) {
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
    Engine::getInstance().recvSegment(conn);
}