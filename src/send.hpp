#include <cstdint>
#include <deque>
#include <format>
#include <cassert>
#include <algorithm>

#include "utils.hpp"

#define SEND_BUFFER_CAPACITY 65536
#define MAX_ISS 1000000
#define DEFAULT_INIT_RWND 4096

struct SendSegment {
    int64_t seqNum;
    int64_t size;

    bool equals(const SendSegment &other) {
        return seqNum == other.seqNum && size == other.size;
    }
};

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

    // ref back to owning Connection
    Connection *connRef;

public:
    SendStream(Connection *conn)
        : connRef(conn), state(SendStreamState::CLOSED) {}

    ~SendStream() {
        free(buffer);
        inFlightSegments.clear();
        state = SendStreamState::CLOSED;
    }

public:
    int64_t getIss() { return iss; }
    int64_t getNxt() { return nxt; }
    int64_t getWnd() { return std::min(cwnd, rwnd); }
    SendStreamState getState() { return state; }

public:
    void setRwnd(int64_t _rwnd) { rwnd = _rwnd; }

public:
    void init(int64_t initRwnd) {
        capacity = SEND_BUFFER_CAPACITY;
        buffer = (uint8_t*)malloc(capacity);
        if (buffer == nullptr) {
            Log(ERROR, std::format("couldn't allocate send buffer - {} bytes", capacity));
            return;
        }

        if (connRef == nullptr) {
            Log(ERROR, std::format("connRef == nullptr"));
            return;
        }

        iss = generateIss();
        una = iss;
        nxt = iss; // first byte to send should be the iss itself (i.e. on initial SYN/SYN-ACK)

        cwnd = INT64_MAX;
        rwnd = initRwnd;

        unaPos = 0;
        nxtPos = 0;
        writePos = 0;

        state = SendStreamState::INITIALISED;
    }

    void init() {
        init(DEFAULT_INIT_RWND);
    }

    void write(int64_t n, uint8_t *inBuffer) {
        if (state != SendStreamState::INITIALISED) {
            Log(ERROR, "send stream not initialised");
            return;
        }

        int64_t currFreeSpace = freeSpaceBytes();
        if (currFreeSpace < n) {
            Log(ERROR, std::format("insufficient free space for write() - {} free bytes, {} bytes to write", currFreeSpace, n));
            return;
        }

        // write new data
        std::memcpy(buffer + writePos, inBuffer, n);
        writePos = (writePos + n) % capacity;

        // new data available => trigger segment-send-attempt
        attemptSegmentSend();
        return;
    }

    bool onHandshakeSynSend() {
        if (nxt != iss) {
            Log(ERROR, std::format("on handshake SYN send, nxt ({}) should be == iss ({})", nxt, iss));
            return false;
        }

        // queue in-flight segment (so we can re-tx if necessary)
        SendSegment seg;
        seg.seqNum = nxt;
        seg.size = 1;
        inFlightSegments.push_back(seg);

        // sending SYN consumes one seqnum
        nxt += 1;

        return true;
    }

    bool onHandshakeFinSend() {
        // queue in-flight segment (so we can re-tx if necessary)
        SendSegment seg;
        seg.seqNum = nxt;
        seg.size = 1;
        inFlightSegments.push_back(seg);

        // sending FIN consumes one seqnum
        nxt += 1;

        return true;
    }

    bool onHandshakeAckRecv(int64_t ackNum) {
        if (!(ackNum == nxt && nxt == iss + 1 && una == iss)) {
            Log(ERROR, std::format("on handshake ACK, should be: ackNum ({}) == nxt ({}) == iss ({}) + 1", ackNum, nxt, iss));
            return;
        }
        una += 1; // peer ack'd ISS => advance una one byte
        return true;
    }

    bool onAck(int64_t ackNum) {
        // ackNum is meaningful if its in the range: [una, nxt], i.e. if its ack'ing unack'd bytes
        if (ackNum < una) {
            Log(ERROR, std::format("received ackNum ({}) < una ({}) - invalid", ackNum, una));
            return false;
        }
        if (ackNum > nxt) {
            Log(ERROR, std::format("received ackNum ({}) > nxt ({}) - invalid", ackNum, nxt));
            return false;
        }

        //
        // TODO - handle triple duplicate ack
        //

        // advance una, removing inFlightSegments as necessary
        while (!inFlightSegments.empty()) {
            SendSegment &seg = inFlightSegments.front();

            if (seg.seqNum + seg.size <= ackNum) {
                //
                // segment fully ack'd - remove segment
                //
                una += seg.size;
                unaPos = (unaPos + seg.size) % capacity;

                inFlightSegments.pop_front();
            } 
            else if (seg.seqNum < ackNum && ackNum < seg.seqNum + seg.size) {
                //
                // segment partially ack'd - update segment
                //
                int64_t ackedBytes = ackNum - seg.seqNum;
                una += ackedBytes;
                unaPos = (unaPos + ackedBytes) % capacity;

                seg.seqNum += ackedBytes;
                seg.size -= ackedBytes;

                break;
            }
        }

        if (una != ackNum) {
            Log(ERROR, std::format("after ACK, una ({}) and ackNum ({}) should be equal", una, ackNum));
            return false;
        }
        if (!bufferStateValid()) {
            return false;
        }

        //
        // Update rto state after ack. 
        // Cancel whatever rto is queued, and queue earliest in-flight segment, if one exists.
        //
        Engine::getInstance().cancelRto(connRef);
        if (!inFlightSegments.empty()) {
            Engine::getInstance().queueRto(connRef, inFlightSegments.front());
        }
    }

     bool onRto(SendSegment seg) {
        //
        // Rto must be for earliest in-flight segment, i.e. front of queue.
        // If not, something is wrong.
        //
        if (inFlightSegments.empty()) {
            Log(ERROR, "RTO generated with no in-flight segments");
            return false;
        }
        SendSegment &earliestSeg = inFlightSegments.front();
        if (!(earliestSeg.seqNum == seg.seqNum && earliestSeg.size == seg.size)) {
            Log(ERROR, "RTO generated for non-front-of-queue segment");
            return false;
        }
        if (!bufferStateValid()) {
            return;
        }

        // re-send segment
        int64_t bufferPos = unaPos;
        int64_t bytesSent = Engine::getInstance().sendSegment(connRef, seg.size, buffer + bufferPos);
        if (bytesSent == -1) {
            return false;
        }

        // segment stays in in-flight queue, una unchanged

        return;
     }

private:
    void attemptSegmentSend() {
        //
        // Send as many 1 MSS segments as we have available
        //
        while (readyToSendBytes() >= MSS) {
            int64_t bytesSent = Engine::getInstance().sendSegment(connRef, MSS, buffer + nxtPos);
            if (bytesSent == -1) {
                return;
            }

            // queue in-flight segment
            SendSegment seg;
            seg.seqNum = nxt;
            seg.size = bytesSent;

            // advance nxt
            nxt += bytesSent;
            nxtPos = (nxtPos + bytesSent) % capacity;
        }

        if (!bufferStateValid()) {
            return;
        }

        //
        // At this point, < 1 MSS available to send.
        // Currently, just wait until more data is available.
        //
        // In future, queue a pending-send to expire after X ms. If still not sent by then, send
        // pending bytes anyway.
        //
    }

    int64_t readyToSendBytes() {
        return ((writePos + capacity) - nxtPos) % capacity;
    }

    int64_t freeSpaceBytes() {
        return ((unaPos + capacity) - writePos) % capacity;
    }

    int64_t generateIss() {
        std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<int64_t> dist(0, MAX_ISS);
        return dist(generator);
    }

    bool bufferStateValid() {
        if ((unaPos != ((una - iss) % capacity))) {
            Log(ERROR, std::format("unaPos ({}) is incorrect buffer position of una ({})", unaPos, una));
            return false;
        }
        if (nxtPos != ((nxt - iss) % capacity)) {
            Log(ERROR, std::format("nxtPos ({}) is incorrect buffer position of nxt ({})", nxtPos, nxt));
            return false;
        }
        if (!inFlightSegments.empty() && inFlightSegments.front().seqNum != una) {
            Log(ERROR, std::format("first in-flight segment seqNum ({}) != una ({})", inFlightSegments.front().seqNum, una));
            return false;
        }
        return true;
    }
};