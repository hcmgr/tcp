#include <cstdint>

#define RECV_BUFFER_CAPACITY 65536

struct ReceivedSegment {
    int64_t seqNum;
    int64_t size;

    int64_t bufferPos;
};

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

    // physical buffer state
    int64_t readPos;
    int64_t nxtPos;

    std::deque<ReceivedSegment> receivedSegments;

    RecvStreamState state;

    // ref back to owning Connection
    Connection *connRef;

public:
    RecvStream()
        : state(RecvStreamState::CLOSED) {}

    ~RecvStream() {
        free(buffer);
        receivedSegments.clear();
        state = RecvStreamState::CLOSED;
    }

public:
    int64_t getNxt() { return nxt; }
    int64_t getWnd() { return freeSpaceBytes(); }

public:
    void init(int64_t _irs) {
        capacity = RECV_BUFFER_CAPACITY;
        buffer = (uint8_t*)malloc(capacity);
        if (buffer == nullptr) {
            Log(ERROR, std::format("couldn't allocate recv buffer - {} bytes", capacity));
            return;
        }

        irs = _irs;
        nxt = irs + 1; // have consumed irs byte => next byte we expect is irs + 1

        readPos = 0;
        nxtPos = 0;

        state = RecvStreamState::INITIALISED;
    }

    void read(int64_t n, uint8_t *outBuffer) {
        int64_t bytesAvail = readyToReadBytes();
        if (n < bytesAvail) {
            //
            // Insufficient data to read.
            //
            // TODO - put to sleep, wake on available data
            //
            Log(INFO, std::format("insufficient data for read() - {} bytes avail, {} bytes to read - sleep until sufficient data available", bytesAvail, n));
            return;
        }

        std::memcpy(outBuffer, buffer + readPos, n);
        readPos += n;
        return;
    }

    void receiveSegment(int64_t seqNum, int64_t n, uint8_t *payload) {
        int64_t freeSpace = freeSpaceBytes();
        if (freeSpace < n) {
            Log(ERROR, std::format("{} bytes exceeds free space - {}", n, freeSpace));
            return;
        }

        if (seqNum < nxt) {
            Log(INFO, std::format("seqNum ({}) < nxt ({}) - already received - ignore"));
            return;
        }

        //
        // Segments can/will be received out of order.
        // So, find its correct location in our 'receiveSegments' deque.
        // Must detect overlaps.
        //
        ReceivedSegment seg;
        seg.seqNum = seqNum;
        seg.size = n;
        seg.bufferPos = (nxtPos + (nxt - seqNum)) % capacity;
    }

private:
    /**
     * For now:
     *      - send acks immediately on receipt of new data
     * Later - implement delayed acks
     *      - queue ack for X ms, waiting to piggyback it onto a sending segment (or another ack)
     */
    void attemptAck() {
        Engine::getInstance().sendAck(connRef);
    }

    int64_t readyToReadBytes() {
        return ((nxtPos + capacity) - readPos) % capacity;
    }

    int64_t freeSpaceBytes() {
        return ((readPos + capacity) - nxtPos) % capacity;
    }
};