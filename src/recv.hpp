#include <cstdint>

#include "utils.hpp"

#define RECV_BUFFER_CAPACITY 65536

struct RecvSegment {
    int64_t seqNum;
    int64_t size;
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
    int64_t read; // next seqnum to read

    // physical buffer state
    int64_t nxtPos;
    int64_t readPos;

    std::deque<RecvSegment> receivedSegments;
    int64_t cumSegmentSize; // cumulative size of `receivedSegments` queue

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
        read = irs + 1;

        readPos = 0;
        nxtPos = 0;

        state = RecvStreamState::INITIALISED;
    }

    void readN(int64_t n, uint8_t *outBuffer) {
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
        read += n;
        readPos += n;
        return;
    }

    void receiveSegment(int64_t seqNum, int64_t n, uint8_t *payload) {
        //
        // received segments only valid in seqnum range: [nxt, read)
        //
        int64_t segL = seqNum;
        int64_t segR = seqNum + n - 1; // inclusive
        if (segL < nxt || read <= segR){
            Log(ERROR, std::format("out of range"));
            return;
        }

        RecvSegment seg;
        seg.seqNum = seqNum;
        seg.size = n;

        //
        // place segment at correct location in our 'receivedSegments' deque (can be out-of-order)
        //
        bool placed = false;
        auto it = receivedSegments.begin();
        while (!receivedSegments.empty()) {
            RecvSegment &curr = *it;
            int64_t currL = curr.seqNum;
            int64_t currR = curr.seqNum + curr.size - 1; // inclusive
            if (segR <= currL) {
                // seg somewhere before curr - place it
                receivedSegments.insert(it, seg);
                cumSegmentSize += seg.size;
                placed = true;
                break;
            } else if (currR < segL) {
                // seg somewhere after curr - wait to place
                continue;
            } else {
                // invalid
                Log(ERROR, std::format("invalid range of new segment: seqNum={}, size={}", seg.seqNum, seg.size));
                return;
            }
        }
        if (!placed) {
            // not placed yet - place at end
            receivedSegments.push_back(seg);
            cumSegmentSize += seg.size;
        }

        //
        // advance nxt as far as segments are contiguous
        //
        while (!receivedSegments.empty()) {
            RecvSegment &curr = receivedSegments.front();
            if (nxt == curr.seqNum) {
                nxt += curr.size;
                nxtPos = (nxtPos + curr.size) % capacity;
                cumSegmentSize -= curr.size;
                receivedSegments.pop_front();
            } else {
                break;
            }
        }

        if (!bufferStateValid()) {
            return;
        }
    }

private:
    //
    // For now:
    //      - send acks immediately on receipt of new data
    // Later, implement delayed acks
    //      - queue ack for X ms, waiting to piggyback it onto a sending segment (or another ack)
    //
    void attemptAck() {
        Engine::getInstance().sendAck(connRef);
    }

    //
    // 'Ready-to-read' space is in front of readPos before nxtPos.
    //
    int64_t readyToReadBytes() {
        return ((nxtPos + capacity) - readPos) % capacity;
    }

    //
    // Free space is in front of nxtPos before readPos.
    // Segments are placed out-of-order in front of nxtPos, but nxtPos only moves once
    // section in front of it is contiguous. So, reduce free space by `cumSegmentSize`.
    //
    int64_t freeSpaceBytes() {
        return (((readPos + capacity) - nxtPos) % capacity) - cumSegmentSize;
    }

    bool bufferStateValid() {
        if (nxtPos != ((nxt - irs) % capacity)) {
            Log(ERROR, std::format("nxtPos ({}) is incorrect buffer position of nxt ({})", nxtPos, nxt));
            return false;
        }
        if (readPos != ((read - irs) % capacity)) {
            Log(ERROR, std::format("readPos ({}) is incorrect buffer position of read ({})", readPos, read));
            return false;
        }
        if (!receivedSegments.empty() && receivedSegments.front().seqNum == nxt) {
            Log(ERROR, std::format("front segment's seqNum should not be == nxt, nxt should have advanced to first non-contiguous segment"));
            return false;
        }
        return true;
    }
};