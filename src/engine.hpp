#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <format>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <thread>

#include <event2/event.h>

#include "tcp_define.hpp"
#include "tcp_conn.hpp"
#include "send.hpp"
#include "recv.hpp"
#include "utils.hpp"

struct Connection {
    // conection id, assigned by Engine, guaranteed unique within this process
    int64_t id;

    // 4-tuple
    std::string srcIp;
    int srcPort;
    std::string destIp;
    int destPort;

    // udp socket fd
    int udpSocketFd;

    // tcp-state-machine state of the connection
    State state;

    // send stream
    SendStream sendStream;

    // recv stream
    RecvStream recvStream;
    uint8_t *recvSegmentBuffer;
    int64_t recvSegmentBufferSize;

    // pending-open state (users sleeping open() calls - waiting to reach ESTABLISHED state)
    std::mutex pendingOpenMutex;
    std::condition_variable pendingOpenCv;

    // pending-read state (users sleeping read() calls - waiting on available data)
    std::mutex pendingReadMutex;
    std::condition_variable pendingReadCv;

    // pending-close state (users sleeping on close() calls - waiting to reach CLOSED state)
    std::mutex pendingCloseMutex;
    std::condition_variable pendingCloseCv;

    // pending-ACK state (delayed ACK - piggyback ACKs onto other segments / ACKs)

    // pending-send state (waiting until 1 MSS available to send)

    Connection(
        int64_t id,
        std::string srcIp,
        int srcPort,
        std::string destIp,
        int destPort,
        int udpSocketFd,
        ConnType connType
    );

    ~Connection();
};

class Engine {
private:
    int64_t connectionIdGen;
    std::unordered_map<int64_t, Connection*> openConnections;

    std::thread thread;
    struct event_base *eventBase;

private:
    Engine();
    ~Engine();
    void eventLoop();

public:
    static Engine& getInstance();

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

public:
    int64_t open(const std::string srcIp,
                  int srcPort, 
                  const std::string destIp,
                  int destPort,
                  ConnType connType);
    void read(int64_t cId, int n, std::vector<uint8_t> &buffer);
    void write(int64_t cId, int n, std::vector<uint8_t> &buffer);
    void close(int64_t cId);

public:
    void onRecvSegment(Connection *conn);
    void onRto(Connection *conn);

    struct event* queueRto(Connection *conn);
    bool cancelRto(Connection *conn, struct event *event);

    Header makeBaseHeader(Connection *conn);

public:
    bool sendHandshakeSyn(Connection *conn);
    bool sendHandshakeSynAck(Connection *conn);
    bool sendHandshakeAck(Connection *conn);
    bool sendAck(Connection *conn);
    bool sendRst(Connection *conn);
    bool sendFin(Connection *conn);

private:
    int createUdpSocket(const std::string& srcIp,
                        int srcPort,
                        const std::string& dstIp,
                        int dstPort);
    bool verifyReceivedHeader(Connection *conn, const Header &hdr);
    bool verifyConn(Connection *conn);
    void reset(Connection *conn);
};

void libeventOnRecvSegment(evutil_socket_t fd, short events, void* arg);
void libeventOnRto(evutil_socket_t fd, short events, void* arg);


