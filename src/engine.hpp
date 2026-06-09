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

struct Rto {
    SendSegment seg;
    bool active;
    struct event *event;
};

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

    // send state
    SendStream sendStream;
    uint8_t *sendSegmentBuffer;
    int64_t sendSegmentBufferSize;

    // recv state
    RecvStream recvStream;
    uint8_t *recvSegmentBuffer;
    int64_t recvSegmentBufferSize;

    // pending-read state (users sleeping read() calls waiting on available data)

    // pending-ACK state (delayed ACK => piggyback ACKs onto other segments / ACKs)

    // rto state
    Rto rto;

    // TODO - re-transmission data structure

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

    int64_t sendSegment(Connection *conn, int64_t n, uint8_t *payloadBuffer);
    int64_t sendSegmentHeaderOnly(Connection *conn, Header &hdr);

    bool sendHandshakeSyn(Connection *conn);
    bool sendHandshakeSynAck(Connection *conn);
    bool sendHandshakeAck(Connection *conn);
    bool sendAck(Connection *conn);
    bool sendRst(Connection *conn);
    bool sendFin(Connection *conn);

    void queueRto(Connection *conn, SendSegment &seg);
    void cancelRto(Connection *conn);
private:
    int createUdpSocket(const std::string& srcIp,
                        int srcPort,
                        const std::string& dstIp,
                        int dstPort);
    
    Header makeBaseHeader(Connection *conn);
    bool verifyReceivedHeader(Connection *conn, const Header &hdr);
    bool verifyConn(Connection *conn);
    void reset(Connection *conn);
};

//////////////////////////////////////////////////////////////////
// Libevent callbacks
//////////////////////////////////////////////////////////////////
void libeventOnRecvSegment(evutil_socket_t fd, short events, void* arg);
void libeventOnRto(evutil_socket_t fd, short events, void* arg);


