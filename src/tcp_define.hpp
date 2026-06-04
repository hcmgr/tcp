#include <cstdint>
#include <string>

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
    BAD                 // errant state - teardown immediately
};
std::string toString(State state);

struct __attribute__((packed)) Header
{
    uint16_t srcPort;       
    uint16_t destPort;
    uint32_t seqNum;
    uint32_t ackNum;

    uint16_t reserved:4;
	uint16_t doff:4;    // [un-used]

	uint16_t SYN:1;     // seqNum == ISS
	uint16_t ACK:1;     // ackNum active
	uint16_t FIN:1;     // finish send of your stream
	uint16_t RST:1;     // teardown connection immediately
	uint16_t PSH:1;     // [un-used]
	uint16_t URG:1;     // [un-used]
	uint16_t RES2:4;    // [reserved]

    uint16_t window;
    uint16_t checksum;
    uint16_t urgPtr;    // [un-used]

    void setChecksum();
    bool verifyChecksum() const;

    void networkToHostOrder();
    void hostToNetworkOrder();

    std::string toString() const;
};