#pragma once
#include <string>
#include <cstdint>

#define MSS                         1460

// receive
#define RECV_BUFFER_CAPACITY        65536
#define DEFAULT_INIT_RWND           4096

// send
#define SEND_BUFFER_CAPACITY        65536
#define RTO_TIMEOUT_MS              50 // ms
#define MAX_ISS                     1 << 28

// congestion
#define INIT_CWND                   (MSS*10)
#define SSTHRESH                    (MSS*20)

enum class tcp_state {
    CLOSED,                 // closed
    LISTEN,                 // listening for incoming connections
    SYN_SENT,               // sent first SYN, waiting on its ACK
    SYN_RECEIVED,           // received first SYN, should now send own SYN
    ESTABLISHED,            // normal send/receive state
    FIN_WAIT_1,             // sent first fin
    CLOSE_WAIT,             // received first fin from peer
    FIN_WAIT_2,             // sent first FIN, ACK'd by other
    TIME_WAIT,              // received second fin from peer, wait 2 MSL to close
    LAST_ACK,               // sent second fin, waiting on ACK
};

constexpr uint16_t fin_mask = 1 << 0;
constexpr uint16_t syn_mask = 1 << 1;
constexpr uint16_t rst_mask = 1 << 2;
constexpr uint16_t ack_mask = 1 << 4;

struct __attribute__((packed)) tcp_header {
    uint16_t src_port;
    uint16_t dest_port;

    uint32_t seqnum;
    uint32_t acknum;

    uint16_t flags;         // active flags: (syn ack fin rst), 4-bit 'data offset' un-used
    uint16_t window;

    uint16_t checksum;
    uint16_t urg_ptr;       // [un-used]

    bool fin() { return flags & fin_mask; }
    bool syn() { return flags & syn_mask; }
    bool rst() { return flags & rst_mask; }
    bool ack() { return flags & ack_mask; }
};