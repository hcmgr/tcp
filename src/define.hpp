#pragma once
#include <string>
#include <cstdint>

// receive
#define RECV_BUFFER_CAPACITY        65536
#define DEFAULT_INIT_RWND           4096

// send
#define SEND_BUFFER_CAPACITY        65536
#define RTO_TIMEOUT_MS              50
#define MAX_ISS                     1 << 28

// congestion
#define INIT_CWND                   (MSS*10)
#define SSTHRESH                    (MSS*20)

// general
#define MSS                         1460
#define DELAYED_ACK_TIMEOUT_MS      40
#define TIME_WAIT_TIMEOUT_MS        60*1000 // 60 seconds

enum class tcp_state {
    CLOSED,                 // closed
    LISTEN,                 // listening for incoming connections
    SYN_SENT,               // sent first SYN, waiting on its ACK
    SYN_RECEIVED,           // received first SYN, should now send own SYN
    ESTABLISHED,            // normal send/receive state
    FIN_WAIT_1,             // sent first fin - waiting on ack
    CLOSE_WAIT,             // received first fin - sent ack - waiting to send YOUR fin
    FIN_WAIT_2,             // your first fin ack'd - waiting on peer's fin
    TIME_WAIT,              // received second fin from peer - sent ack - wait 2MSL to close
    LAST_ACK,               // sent second fin - waiting on ack to close
    CLOSING,                // sent own fin, received peer's fin (simultaneous close case)
};

inline std::string to_string(tcp_state state) {
    switch (state) {
        case tcp_state::CLOSED:       return "CLOSED";
        case tcp_state::LISTEN:       return "LISTEN";
        case tcp_state::SYN_SENT:     return "SYN_SENT";
        case tcp_state::SYN_RECEIVED: return "SYN_RECEIVED";
        case tcp_state::ESTABLISHED:  return "ESTABLISHED";
        case tcp_state::FIN_WAIT_1:   return "FIN_WAIT_1";
        case tcp_state::CLOSE_WAIT:   return "CLOSE_WAIT";
        case tcp_state::FIN_WAIT_2:   return "FIN_WAIT_2";
        case tcp_state::TIME_WAIT:    return "TIME_WAIT";
        case tcp_state::LAST_ACK:     return "LAST_ACK";
    }
    return "UNKNOWN";
}

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

    std::string to_string() {
        return "";
    }
};

enum class conn_type {
    CONNECT,
    LISTEN
};