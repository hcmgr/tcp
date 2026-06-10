#include <cstring>
#include <cerrno>
#include <random>
#include <sys/socket.h>

#include <event2/event.h>

#include "engine.hpp"

SendStream::SendStream(Connection *conn)
    : connRef(conn), state(SendStreamState::CLOSED) {}

SendStream::~SendStream() {
    free(buffer);
    free(preSendBuffer);
    inFlightSegments.clear();
    state = SendStreamState::CLOSED;
}

//////////////////////////////////////////////////////////////////
// getters / setters
//////////////////////////////////////////////////////////////////

int64_t SendStream::getIss() { return iss; }
int64_t SendStream::getNxt() { return nxt; }
int64_t SendStream::getWnd() { return std::min(cwnd, rwnd); }
SendStreamState SendStream::getState() { return state; }
void SendStream::setRwnd(int64_t _rwnd) { rwnd = _rwnd; }


//////////////////////////////////////////////////////////////////
// main public api:
//      init()
//      sendSegment()
//      write()
//      onAck()
//      onRto()
//////////////////////////////////////////////////////////////////

void SendStream::init(int64_t initRwnd) {
    capacity = SEND_BUFFER_CAPACITY;
    buffer = (uint8_t*)malloc(capacity);
    if (buffer == nullptr) {
        Log(ERROR, std::format("couldn't allocate send buffer - {} bytes", capacity));
        return;
    }

    preSendBuffer = (uint8_t*)malloc(MSS);
    if (preSendBuffer == nullptr) {
        Log(ERROR, std::format("couldn't allocate pre-send buffer - {} bytes", MSS));
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

void SendStream::init() {
    init(DEFAULT_INIT_RWND);
}

int64_t SendStream::sendSegment(Header &hdr, int64_t payloadSize) {
    if (payloadSize > MSS) {
        Log(ERROR, std::format("cannot send payload size > 1 MSS - {}", payloadSize));
        return -1;
    }
    int bytesToSend = sizeof(hdr) + payloadSize;
    if (getWnd() < bytesToSend) {
        Log(INFO, std::format("no send attempt - wnd (cwnd={}, rwnd={}) < bytesToSend ({})", cwnd, rwnd, bytesToSend));
        return -1;
    }

    // write header + payload to intermediate buffer
    std::memcpy(preSendBuffer, &hdr, sizeof(hdr));
    if (payloadSize > 0) {
        int64_t payloadPos = nxtPos;
        readFromBuffer(payloadPos, preSendBuffer + sizeof(hdr), payloadSize);
    }

    // send segment
    int bytesSent = send(connRef->udpSocketFd, preSendBuffer, bytesToSend, 0);
    if (bytesSent == -1) {
        Log(ERROR, std::format("error on send() - {}", strerror(errno)));
        return -1;
    }
    if (bytesSent != bytesToSend) {
        Log(ERROR, std::format("partial send() - attempted {}, sent {}", bytesToSend, bytesSent));
        return -1;
    }

    // advance nxt
    int64_t seqNumSize = payloadSize;
    if (hdr.SYN || hdr.FIN) {
        seqNumSize += 1;
    }
    nxt += seqNumSize;
    nxtPos = (nxt - iss) % capacity;

    // queue in-flight segment
    SendSegment seg;
    seg.hdr = hdr;
    seg.seqNum = hdr.seqNum;
    seg.payloadSize = payloadSize;
    seg.seqNumSize = seqNumSize;

    inFlightSegments.push_back(seg);

    // queue rto if: a) no active RTO, and b) segment consumes seqnums
    if (!rto.active && seqNumSize > 0) {
        queueRto(seg);
    }

    return bytesSent;
}

void SendStream::write(int64_t n, uint8_t *inBuffer) {
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
    writeToBuffer(writePos, inBuffer, n);
    writePos = (writePos + n) % capacity;

    // new data available => trigger segment-send-attempt
    attemptSegmentSend();
}

bool SendStream::onAck(int64_t ackNum) {
    // ackNum is meaningful if its in the range: [una, nxt), i.e. if its ack'ing unack'd bytes
    if (ackNum < una) {
        Log(ERROR, std::format("received ackNum ({}) < una ({}) - invalid", ackNum, una));
        return false;
    }
    if (ackNum >= nxt) {
        Log(ERROR, std::format("received ackNum ({}) > nxt ({}) - invalid", ackNum, nxt));
        return false;
    }

    //
    // TODO - handle triple duplicate ack
    //

    // advance una, removing inFlightSegments as necessary
    while (!inFlightSegments.empty()) {
        SendSegment &seg = inFlightSegments.front();

        if (seg.seqNum + seg.seqNumSize <= ackNum) {
            //
            // segment fully ack'd - remove segment
            //
            una += seg.seqNumSize;
            unaPos = (una - iss) % capacity;

            inFlightSegments.pop_front();
        }
        else if (seg.seqNum < ackNum && ackNum < seg.seqNum + seg.seqNumSize) {
            //
            // segment partially ack'd - update segment
            //
            int64_t ackedBytes = ackNum - seg.seqNum;
            una += ackedBytes;
            unaPos = (unaPos + ackedBytes) % capacity;

            seg.seqNum += ackedBytes;
            seg.hdr.seqNum += ackedBytes;
            seg.seqNumSize -= ackedBytes;

            break;
        } else {
            Log(ERROR, std::format("segment neither fully ack'd nor partially ack'd"));
            return false;
        }
    }

    if (una != ackNum) {
        Log(ERROR, std::format("after ACK, una ({}) and ackNum ({}) should be equal", una, ackNum));
        return false;
    }
    if (!bufferStateValid()) {
        return false;
    }

    // restart rto with new oldest un-ack'd segment
    bool res = cancelRto();
    if (!res) {
        return false;
    }
    if (!inFlightSegments.empty()) {
        res = queueRto(inFlightSegments.front());
        if (!res) {
            return false;
        }
    }

    // ack means more data available => attempt segment sends
    attemptSegmentSend();

    return true;
}

bool SendStream::onRto() {
    //
    // Rto must be for earliest in-flight segment, i.e. front of queue.
    // If not, something is wrong.
    //
    if (inFlightSegments.empty()) {
        Log(ERROR, "RTO generated with no in-flight segments");
        return false;
    }
    SendSegment &rtoSeg = rto.seg;
    SendSegment &earliestSeg = inFlightSegments.front();
    if (!(earliestSeg.seqNum == rtoSeg.seqNum && earliestSeg.seqNumSize == rtoSeg.seqNumSize)) {
        Log(ERROR, "RTO generated for non-front-of-queue segment");
        return false;
    }
    if (!bufferStateValid()) {
        return false;
    }

    // retransmit segment
    int64_t bytesSent = retransmitSegment(rtoSeg);
    if (bytesSent == -1) {
        return false;
    }

    // restart rto for same segment
    bool res = cancelRto();
    if (!res) {
        return false;
    }
    res = queueRto(rtoSeg);
    if (!res) {
        return false;
    }

    return true;
}

//////////////////////////////////////////////////////////////////
// private
//////////////////////////////////////////////////////////////////

void SendStream::attemptSegmentSend() {
    //
    // send as many 1 MSS segments as we have available
    //
    while (readyToSendBytes() >= MSS) {
        Header hdr = Engine::getInstance().makeBaseHeader(connRef);
        hdr.ACK = 1;

        int64_t bytesSent = sendSegment(hdr, MSS);
        if (bytesSent == -1) {
            return;
        }
    }

    //
    // At this point, < 1 MSS available to send.
    // Currently, just wait until more data is available.
    //
    // In future, queue a pending-send to expire after X ms. If still not sent by then, send
    // pending bytes anyway.
    //
}

int64_t SendStream::retransmitSegment(SendSegment &seg) {
    if (getWnd() < MSS) {
        Log(INFO, std::format("no send attempt - wnd < MSS (cwnd={}, rwnd={})", cwnd, rwnd));
        return -1;
    }

    std::memcpy(preSendBuffer, &seg.hdr, sizeof(seg.hdr));
    if (seg.payloadSize > 0) {
        int64_t payloadPos = (unaPos + (seg.seqNum - una)) % capacity;
        readFromBuffer(payloadPos, preSendBuffer + sizeof(seg.hdr), seg.payloadSize);
    }

    int bytesToSend = sizeof(seg.hdr) + seg.payloadSize;
    int bytesSent = send(connRef->udpSocketFd, preSendBuffer, bytesToSend, 0);
    if (bytesSent == -1) {
        Log(ERROR, std::format("error on send() - {}", strerror(errno)));
        return -1;
    }
    if (bytesSent != bytesToSend) {
        Log(ERROR, std::format("partial send() - attempted {}, sent {}", bytesToSend, bytesSent));
        return -1;
    }

    // segment stays in in-flight queue, and una unchanged
}

bool SendStream::queueRto(SendSegment &seg) {
    struct event *event = Engine::getInstance().queueRto(connRef);
    if (!event) {
        return false;
    }
    rto.seg = seg;
    rto.event = event;
    rto.active = true;

    return true;
}

bool SendStream::cancelRto() {
    if (rto.active) {
        bool res = Engine::getInstance().cancelRto(connRef, rto.event);
        if (!res) {
            return false;
        }
        event_free(rto.event);
        rto.event = nullptr;
        rto.active = false;
    }

    return true;
}

void SendStream::writeToBuffer(int64_t pos, uint8_t *src, int64_t n) {
    int64_t firstChunk = std::min(n, capacity - pos);
    std::memcpy(buffer + pos, src, firstChunk);
    if (n > firstChunk) {
        std::memcpy(buffer, src + firstChunk, n - firstChunk);
    }
}

void SendStream::readFromBuffer(int64_t pos, uint8_t *dest, int64_t n) {
    int64_t firstChunk = std::min(n, capacity - pos);
    std::memcpy(dest, buffer + pos, firstChunk);
    if (n > firstChunk) {
        std::memcpy(dest + firstChunk, buffer, n - firstChunk);
    }
}

int64_t SendStream::readyToSendBytes() {
    return ((writePos + capacity) - nxtPos) % capacity;
}

int64_t SendStream::freeSpaceBytes() {
    return ((unaPos + capacity) - writePos) % capacity;
}

int64_t SendStream::generateIss() {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int64_t> dist(0, MAX_ISS);
    return dist(generator);
}

bool SendStream::bufferStateValid() {
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
