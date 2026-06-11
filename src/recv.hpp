#include <cstdint>
#include <deque>

#include "utils.hpp"

struct RecvSegment;

enum class RecvStreamState {
    CLOSED,
    INITIALISED
};

/**
 * RecvStream implemented as a circular buffer.
 *
 * Layout
 *      {READ_POS}
 *      [contiguous bytes, ready to be read by user]
 *      {NXT_POS}
 *      [free space to write segments - segments can be received out of order]
 *          [empty slot for segment i]
 *          [empty slot for segment i+1]
 *          [segment i+2]
 *          [segment i+3]
 *          [empty slot for segment i+4]
 *          ... - circles back
 *
 * Even though the recv stream is a CONTINUOUS byte stream, we keep track of our DISCETE
 * received segments. This is so that, if/when they arrive out of order, we can stitch
 * the segments together properly. We achieve this with the `receivedSegments` deque.
 * Received segments are added onto the queue when they arrive, and taken off when they are read.
 * Importantly, nxt/nxtPos only moves if the next contiguous segment arrives.
 * For example:
 *      [nxt == 1000]
 *      segment 1000-1500 arrives [nxt == 1500]
 *      segment 2000-2500 arrives [nxt == 1500]
 *      segment 2500-3000 arrives [nxt == 1500]
 *      segment 1500-2000 arrives [nxt == 3000]
 *
 * Responsibilities:
 *      - process user read()'s
 *      - receive segments
 *      - send ACKs of received segments
 */
class RecvStream {
private:
    uint8_t *buffer;
    int64_t capacity;

    // logical seqnum state
    int64_t irs; // initial receive seqnum
    int64_t nxt; // next seqnum to receive
    int64_t read_; // next seqnum to read

    // physical buffer state
    int64_t nxtPos;
    int64_t readPos;

    std::deque<RecvSegment> pendingSegments;
    int64_t cumPendingSegmentsSize;

    RecvStreamState state;

    // ref back to owning Connection
    Connection *connRef;

public:
    RecvStream(Connection *conn);
    ~RecvStream();

public:
    int64_t getNxt();
    int64_t getWnd();

public:
    void init(int64_t _irs);
    int64_t read(int64_t n, uint8_t *outBuffer);
    void receiveSegment(RecvSegment &hdr, uint8_t *payloadPtr);

    int64_t readyToReadBytes();
    int64_t freeSpaceBytes();

private:
    void writeToBuffer(int64_t pos, uint8_t *src, int64_t n);
    void readFromBuffer(int64_t pos, uint8_t *dest, int64_t n);
    void attemptAck();
};

struct RecvSegment {
    Header hdr;
    int64_t size;

    std::string toString() const;
};
