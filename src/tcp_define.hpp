#include <cstdint>

#define MSS 1460

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

struct __attribute__((packed)) Header
{
    uint16_t srcPort;
    uint16_t destPort;
    uint32_t seqNum;
    uint32_t ackNum;

    uint16_t reserved:4;
	uint16_t doff:4;

	uint16_t FIN:1;
	uint16_t SYN:1;
	uint16_t RST:1;
	uint16_t PSH:1;
	uint16_t ACK:1;
	uint16_t URG:1;
	uint16_t RES2:2;

    uint16_t window;
    uint16_t checksum;
    uint16_t urgPtr;

    void networkToHostOrder();
    void hostToNetworkOrder();
};