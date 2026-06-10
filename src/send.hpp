#include <cstdint>
#include <deque>
#include <format>
#include <cassert>
#include <algorithm>

#include "utils.hpp"

struct SendSegment;
struct Rto;

enum class SendStreamState {
    CLOSED,
    INITIALISED
};

/**
 * SendStream implemented as a circular buffer.
 *
 * Layout
 *      [free space]
 *      {UNA_POS}
 *      [bytes written, sent, not yet ack'd, logically divided up into the segments we've sent]
 *          segment i
 *          segment i+1
 *          segment i+2 (sent, not yet ack'd)
 *      {NXT_POS}
 *      [bytes written, not yet sent]
 *      {WRITE_POS}
 *      [free space]
 *      {UNA_POS + WND}
 *      [free space] - circles back
 *
 * Even though the send stream is a CONTINUOUS byte stream, we keep track of our DISCRETE
 * in-flight segments. This is so that, when re-transmitting, we know exactly what segments to
 * re-send. We achieve this with the `inFlightSegments` deque. Sent segments are added onto
 * the queue. Once fully ack'd, they are taken off.
 *
 * Responsibilities:
 *      - send segments
 *      - process user write()'s
 *      - process ACK
 *          - triple duplicate ACK => re-transmit offending segment
 *          - normal ACK
 *      - process RTO
 *          - re-transmit offending segment
 */
class SendStream {
private:
    uint8_t *buffer;
    int64_t capacity;

    // write out header + segment here before send() call
    uint8_t *preSendBuffer;

    // logical seqnum state
    int64_t iss; // initial send seqnum
    int64_t una; // first sent and un-ack'd seqnum
    int64_t nxt; // next seqnum to send

    // physical buffer state
    int64_t unaPos;
    int64_t nxtPos;
    int64_t writePos;

    // wnd == min(cwnd, rwnd) == max un-ack'd bytes that can be in-flight
    int64_t cwnd;
    int64_t rwnd;

    std::deque<SendSegment> inFlightSegments;

    SendStreamState state;

    // retransmission
    Rto rto;

    // ref back to owning Connection
    Connection *connRef;

public:
    SendStream(Connection *conn);
    ~SendStream();

public:
    int64_t getIss();
    int64_t getNxt();
    int64_t getWnd();
    SendStreamState getState();
    void setRwnd(int64_t _rwnd);

public:
    void init(int64_t initRwnd);
    void init();

    int64_t sendSegment(Header &hdr, int64_t payloadSize);
    void write(int64_t n, uint8_t *inBuffer);
    bool onAck(int64_t ackNum);
    bool onRto();

private:
    void writeToBuffer(int64_t pos, uint8_t *src, int64_t n);

    void attemptSegmentSend();
    int64_t retransmitSegment(SendSegment &seg);

    bool queueRto(SendSegment &seg);
    bool cancelRto();

    int64_t readyToSendBytes();
    int64_t freeSpaceBytes();
    int64_t generateIss();
    bool bufferStateValid();
};

struct SendSegment {
    Header hdr;
    int64_t seqNum;
    int64_t payloadSize;
    int64_t seqNumSize; // total seqnum space it takes up, i.e. payload + SYN/FIN 1-byte contribution
};

struct Rto {
    SendSegment seg;
    bool active;
    struct event *event;
};
