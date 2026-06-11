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

//////////////////////////////////////////////////////////////////
// Connection
//////////////////////////////////////////////////////////////////

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
    recvStream(this)
{
    if (connType == ConnType::LISTEN) {
        state = State::LISTEN;
    } else if (connType == ConnType::CONNECT) {
        state = State::CLOSED;
    } else {
        Log(ERROR, std::format("incorrect connType: {} - defaulting to closed", toString(connType)));
        state = State::INVALID;
        return;
    }

    recvSegmentBufferSize = sizeof(Header) + MSS;
    recvSegmentBuffer = (uint8_t*)malloc(recvSegmentBufferSize);
    if (recvSegmentBuffer == nullptr) {
        Log(ERROR, std::format("issue allocating recv segment buffer - {}", recvSegmentBuffer));
        state = State::INVALID;
        return;
    }
}

Connection::~Connection() {
    free(recvSegmentBuffer);
    state = State::INVALID;
}

//////////////////////////////////////////////////////////////////
// Engine
//////////////////////////////////////////////////////////////////

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
// Engine's open/read/write/close API - called by TcpConn
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
    if (conn->state == State::INVALID) {
        Log(ERROR, "error initialising Connection object");
        return -1;
    }

    // register the 'recvSegment' event
    struct event *recvSegment = event_new(eventBase, udpSocketFd, EV_READ|EV_PERSIST, libeventOnRecvSegment, (void*)conn);
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
        // invalid starting state
        reset(conn);
        return -1;
    }

    if (openConnections.count(cId) != 0) {
        Log(ERROR, std::format("connection already exists with id={}", cId));
        return -1;
    }
    openConnections[cId] = conn;

    // put open() call to sleep, waiting on connection to move out of handshake state
    std::unique_lock<std::mutex> ul(conn->pendingOpenMutex);
    conn->pendingOpenCv.wait(ul, [&] { 
        State state = conn->state;
        return (
            state != State::LISTEN && 
            state != State::SYN_SENT &&
            state != State::SYN_RECEIVED
        );
    });

    // wake up - verify in established state
    if (conn->state != State::ESTABLISHED) {
        if (conn->state == State::CLOSED) {
            // already torn down - do nothing
        } else {
            // bad state - teardown
            reset(conn);
        }
        return -1;
    }

    // connection successfully reached established state
    connectionIdGen++;
    return cId;
}

int64_t Engine::read(int64_t cId, int n, uint8_t *outBuffer) {
    auto it = openConnections.find(cId);
    if (it == openConnections.end()) {
        Log(ERROR, std::format("couldn't find connection: {}", cId));
        return -1;
    }
    Connection* conn = it->second;

    int64_t bytesAvail = conn->recvStream.readyToReadBytes();
    if (bytesAvail >= n) {
        int64_t bytesRead = conn->recvStream.read(n, outBuffer);
        if (bytesRead != n) {
            Log(ERROR, std::format("bytes read ({}) != bytes requested ({}), despite sufficient bytes ready ({})", bytesRead, n, bytesAvail));
            return -1;
        }
        return n;
    }

    // sleep until sufficient data is ready
    std::unique_lock<std::mutex> ul(conn->pendingReadMutex);
    conn->pendingReadCv.wait(ul, [&] { return conn->recvStream.readyToReadBytes() >= n; });

    //
    // TODO - wake up, process ready data
    //

    return 0;
}

int64_t Engine::write(int64_t cId, int n, uint8_t *inBuffer) {
    auto it = openConnections.find(cId);
    if (it == openConnections.end()) {
        Log(ERROR, std::format("couldn't find connection: {}", cId));
        return -1;
    }
    Connection* conn = it->second;

    int64_t freeSpaceAvail = conn->sendStream.freeSpaceBytes();
    if (freeSpaceAvail >= n) {
        int64_t bytesWritten = conn->sendStream.write(n, inBuffer);
        if (bytesWritten != n) {
            Log(ERROR, std::format("bytes written ({}) != bytes requested ({}), despite sufficient free space ({})", bytesWritten, n, freeSpaceAvail));
            return -1;
        }
        return n;
    }
    
    // insufficient space - return 0, and user can decide when to try again later
    return 0;
}

void Engine::close(int64_t cId) {
    auto it = openConnections.find(cId);
    if (it == openConnections.end()) {
        Log(ERROR, std::format("couldn't find connection: {}", cId));
        return;
    }
    Connection* conn = it->second;

}

//////////////////////////////////////////////////////////////////
// Rest of Engine's API - used by internal tcp components, e.g.
// send/recv streams, libevent callbacks, etc.
//////////////////////////////////////////////////////////////////

void Engine::onRecvSegment(Connection *conn) {
    Log(INFO, std::format("segment arrive - state={}", toString(conn->state)));

    if (!verifyConn(conn)) {
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
        reset(conn);
        return;
    }
    if (conn->sendStream.getState() == SendStreamState::INITIALISED) {
        conn->sendStream.setRwnd(hdr.window);
    }

    switch (conn->state) {
        case State::LISTEN: {
            if (hdr.FIN || hdr.RST) {
                Log(ERROR, std::format("state=LISTEN - FIN/RST set - invalid - sending RST to teardown connection - {}", hdr.toString()));
                reset(conn);
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
                    reset(conn);
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
                reset(conn);
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
                bool res = conn->sendStream.onAck(hdr.ackNum);
                if (!res) {
                    reset(conn);
                    return;
                }

                // send ACK
                sendHandshakeAck(conn);

                // enter stablished state - wake any pending open() calls
                conn->state = State::ESTABLISHED;
                conn->pendingOpenCv.notify_one();
            } else {
                Log(ERROR, std::format("state=SYN-SENT, didn't receive SYN-ACK - invalid - sending RST - {}", hdr.toString()));
                reset(conn);
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
                bool res = conn->sendStream.onAck(hdr.ackNum);
                if (!res) {
                    reset(conn);
                    return;
                }

                // enter stablished state - wake any pending open() calls
                conn->state = State::ESTABLISHED;
                conn->pendingOpenCv.notify_one();
            } else {
                Log(ERROR, "state=SYN-RECEIVED, segment is not an ACK - invalid");
                reset(conn);
                return;
            }
        } break;
        case State::ESTABLISHED: {
            if (hdr.FIN || hdr.RST) {
                return;
            }
            if (hdr.SYN) {
                Log(ERROR, std::format("state=ESTABLISHED - SYN bit set - invalid"));
                reset(conn);
                return;
            }

            //
            // handle ACK
            //
            if (hdr.ACK) {
                bool res = conn->sendStream.onAck(hdr.ackNum);
                if (!res) {
                    Log(ERROR, std::format("state=ESTABLISHED, invalid ACK - {}", hdr.toString()));
                    reset(conn);
                    return;
                }
            }

            //
            // handle payload itself
            //
            int payloadSize = bytesRead - sizeof(hdr);
            uint8_t *payloadPtr = conn->recvSegmentBuffer + sizeof(hdr);

            RecvSegment seg;
            seg.hdr = hdr;
            seg.size = payloadSize;

            conn->recvStream.receiveSegment(seg, payloadPtr);
        } break;

        case State::FIN_WAIT_1: {
            // verify ack of fin, move to FIN_WAIT_2
        } break;
        case State::CLOSE_WAIT: {
            // In CLOSE_WAIT state, only receiving ACKs of your data stream.
            // Move from CLOSE_WAIT to LAST_ACK when send last FIN.
        } break;
        case State::FIN_WAIT_2: {
            // receive and ack peer's fin, move to TIME_WAIT
        } break;
        case State::TIME_WAIT: {
        } break;
        case State::LAST_ACK: {
            // receive ack of fin, move to CLOSED
        } break;
    }
}

void Engine::onRto(Connection *conn) {
    if (!verifyConn(conn)) return;

    bool res = conn->sendStream.onRto();
    if (!res) {
        reset(conn);
        return;
    }
}

struct event* Engine::queueRto(Connection *conn) {
    if (!verifyConn(conn)) return nullptr;

    struct timeval t;
    t.tv_usec = RTO_TIMEOUT_MS * 1000;
    struct event *rto = event_new(eventBase, NULL, EV_TIMEOUT, libeventOnRto, (void*)conn);
    if (!rto) {
        Log(ERROR, "bad rto event create - error on event_new()");
        return nullptr;
    }
    event_add(rto, &t);

    return rto;
}

bool Engine::cancelRto(Connection *conn, struct event *event) {
    if (!verifyConn(conn)) return false;

    if (event == nullptr) {
        Log(INFO, "given event is null");
        return false;
    }

    int res = event_del(event);
    if (res == -1) {
        Log(ERROR, "bad rto cancel - error on event_del()");
        return false;
    }
    event_free(event);

    return true;
}

Header Engine::makeBaseHeader(Connection *conn) {
    if (!verifyConn(conn)) return;

    Header hdr{};
    hdr.srcPort = conn->srcPort;
    hdr.destPort = conn->destPort;
    hdr.seqNum = conn->sendStream.getNxt();
    hdr.ackNum = conn->recvStream.getNxt();
    hdr.window = conn->recvStream.getWnd();

    return hdr;
}

bool Engine::sendHandshakeSyn(Connection *conn) {
    if (!verifyConn(conn)) return false;

    Header hdr = makeBaseHeader(conn);
    hdr.SYN = 1;
    hdr.setChecksum();

    SendSegment seg;
    seg.hdr = hdr;
    seg.size = 0;

    int64_t sent = conn->sendStream.sendNextSegment(seg);
    if (sent == -1) {
        return false;
    }

    return true;
}

bool Engine::sendHandshakeSynAck(Connection *conn) {
    if (!verifyConn(conn)) return false;

    Header hdr = makeBaseHeader(conn);
    hdr.SYN = 1;
    hdr.ACK = 1;
    hdr.setChecksum();

    SendSegment seg;
    seg.hdr = hdr;
    seg.size = 0;

    int64_t sent = conn->sendStream.sendNextSegment(seg);
    if (sent == -1) {
        return false;
    }

    return true;
}

bool Engine::sendHandshakeAck(Connection *conn) {
    if (!verifyConn(conn)) return false;

    Header hdr = makeBaseHeader(conn);
    hdr.ACK = 1;
    hdr.setChecksum();

    SendSegment seg;
    seg.hdr = hdr;
    seg.size = 0;

    int64_t sent = conn->sendStream.sendNextSegment(seg);
    if (sent == -1) {
        return false;
    }

    return true;
}

bool Engine::sendAck(Connection *conn) {
    if (!verifyConn(conn)) return false;

    Header hdr = makeBaseHeader(conn);
    hdr.ACK = 1;
    hdr.setChecksum();

    SendSegment seg;
    seg.hdr = hdr;
    seg.size = 0;

    int64_t sent = conn->sendStream.sendNextSegment(seg);
    if (sent == -1) {
        return false;
    }

    return true;
}

bool Engine::sendRst(Connection *conn) {
    if (!verifyConn(conn)) return false;

    Header hdr;
    hdr.srcPort = conn->srcPort;
    hdr.destPort = conn->destPort;
    hdr.RST = 1;
    hdr.setChecksum();

    SendSegment seg;
    seg.hdr = hdr;
    seg.size = 0;

    int64_t sent = conn->sendStream.sendNextSegment(seg);
    if (sent == -1) {
        return false;
    }

    return true;
}

bool Engine::sendFin(Connection *conn) {
    if (!verifyConn(conn)) return false;

    Header hdr = makeBaseHeader(conn);
    hdr.ACK = 1;
    hdr.FIN = 1;
    hdr.setChecksum();

    SendSegment seg;
    seg.hdr = hdr;
    seg.size = 0;

    int64_t sent = conn->sendStream.sendNextSegment(seg);
    if (sent == -1) {
        return false;
    }

    return true;
}

//////////////////////////////////////////////////////////////////
// private helpers
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

bool Engine::verifyConn(Connection *conn) {
    if (!conn) {
        Log(ERROR, "conn null");
        return false;
    }
    auto it = openConnections.find(conn->id);
    if (it == openConnections.end() || it->second != conn) {
        Log(ERROR, "engine has no matching open connection");
        return false;
    }
    return true;
}

void Engine::reset(Connection *conn) {
    sendRst(conn);
    conn->state = State::CLOSED;
    openConnections.erase(conn->id);
}

//////////////////////////////////////////////////////////////////
// Libevent callbacks
//////////////////////////////////////////////////////////////////

void libeventOnRecvSegment(evutil_socket_t fd, short events, void* arg) {
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
    Engine::getInstance().onRecvSegment(conn);
}

void libeventOnRto(evutil_socket_t fd, short events, void* arg) {
    if (!(events & EV_TIMEOUT)) {
        Log(ERROR, "EV_READ not triggered");
        return;
    }
    Connection *conn = (Connection*)arg;
    Engine::getInstance().onRto(conn);
}