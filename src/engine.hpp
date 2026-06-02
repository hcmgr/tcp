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

struct PendingAck {
    // ackNum
    // event id / handle of ACK-timeout event
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

    // send stream
    SendStream sendStream;

    // recv stream
    RecvStream recvStream;

    // pending-read state (users sleeping read() calls waiting on available data)

    // pending-ACK state (delayed ACK => piggyback ACKs onto other segments / ACKs)

    // TODO - re-transmission data structure

    Connection();
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
    void handleSegmentArrive(Connection *conn);

private:
    int createUdpSocket(const std::string& srcIp,
                        int srcPort,
                        const std::string& dstIp,
                        int dstPort);
    
    int handleHandshakeSyn(const Header &hdr);
    int handleHandshakeSynAck(const Header &hdr);
    int handleHandshakeAck(const Header &hdr);
    
};

//////////////////////////////////////////////////////////////////
// Libevent callbacks
//////////////////////////////////////////////////////////////////
void libeventSegmentArrive(evutil_socket_t fd, short events, void* arg);
