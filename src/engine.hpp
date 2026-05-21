#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <format>

#include <event2/event.h>

#include "tcp_conn.hpp"
#include "utils.hpp"

enum class State {
    CLOSED,             // closed
    LISTEN,             // listening for incoming connections
    SYN_SENT,           // sent first SYN, waiting on its ACK
    SYN_RECEIVED,       // received first SYN, should now send own SYN
    ESTABLISHED,        // normal send/receive state
    FIN_WAIT_1,         // sent first fin
    CLOSE_WAIT,         // received first fin from peer
    FIN_WAIT_2,         // sent first FIN, ACK'd by other
    TIME_WAIT,          // received second fin from peer, wait 2 MSL to close
    LAST_ACK,           // sent second fin, waiting on ACK
};

struct Connection {
    // conection id, assigned by Engine, guaranteed unique within this process
    int id;

    // 4-tuple
    const std::string &srcIp;
    const int srcPort;
    const std::string &destIp;
    const int destPort;

    // udp socket fd
    int udpSocketFd;

    // tcp-state-machine state of the connection
    State state;

    // send buffer
    std::vector<uint8_t> sendBuffer;
    int64_t SND_UNA;
    int64_t SND_NXT;
    int64_t SND_WND;

    // recv buffer
    std::vector<uint8_t> recvBuffer;
    int64_t RCV_NXT;
    int64_t RCV_WND;

    // re-tx data structure

};

/**
 * Global, singleton tcp engine.
 */
class Engine {
private:
    int64_t connectionIdGen;
    std::unordered_map<int64_t, Connection*> openConnections;

    std::thread thread;
    struct event_base *eventBase;

private:
    Engine();
    void eventLoop();

public:
    static Engine& getInstance();

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

public:
    int64_t open(const std::string &srcIp,
                  int srcPort, 
                  const std::string &destIp,
                  int destPort,
                  ConnType connType);
    void read(int64_t cId, int n, std::vector<uint8_t> &buffer);
    void write(int64_t cId, int n, std::vector<uint8_t> &buffer);
    void close(int64_t cId);
};