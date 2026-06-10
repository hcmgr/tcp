#include <cstring>
#include <cerrno>
#include <random>
#include <sys/socket.h>

#include <event2/event.h>

#include "engine.hpp"

SendStream::SendStream(Connection *conn)
    : state(SendStreamState::CLOSED), connRef(conn) {}

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
    write_ = iss;

    // physical positions are 1:1 with the logical seqnums
    unaPos = 0;
    nxtPos = 0;
    writePos = 0;

    cwnd = INT64_MAX;
    rwnd = initRwnd;

    state = SendStreamState::INITIALISED;

    rto.active = false;
    rto.event = nullptr;
}

void SendStream::init() {
    init(DEFAULT_INIT_RWND);
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
    write_ += n;
    writePos = (write_ - iss) % capacity;

    // new data available - send ready segments
    sendReadySegments();
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

        if (seg.seqNum + seg.size <= ackNum) {
            //
            // segment fully ack'd - remove segment
            //
            una += seg.size;
            unaPos = (una - iss) % capacity;

            inFlightSegments.pop_front();
        }
        else if (seg.seqNum < ackNum && ackNum < seg.seqNum + seg.size) {
            //
            // segment partially ack'd - update segment
            //
            int64_t ackedBytes = ackNum - seg.seqNum;
            una += ackedBytes;
            unaPos = (una - iss) % capacity;

            seg.seqNum += ackedBytes;
            seg.hdr.seqNum += ackedBytes;
            seg.size -= ackedBytes;

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

    // new data available - send ready segments
    sendReadySegments();

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
    if (!(earliestSeg.seqNum == rtoSeg.seqNum && earliestSeg.size == rtoSeg.size)) {
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

void SendStream::sendReadySegments() {
    //
    // send as many 1 MSS segments as we have available
    //
    while (readyToSendBytes() >= MSS) {
        Header hdr = Engine::getInstance().makeBaseHeader(connRef);
        hdr.ACK = 1;

        int64_t bytesSent = sendNextSegment(hdr, MSS);
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

//
// Send segment with header `hdr` and payload of `payloadSize` bytes taken from nxt onwards.
//
int64_t SendStream::sendNextSegment(Header &hdr, int64_t payloadSize) {
    if (payloadSize > MSS) {
        Log(ERROR, std::format("cannot send payload size > 1 MSS - {}", payloadSize));
        return -1;
    }
    int bytesToSend = sizeof(hdr) + payloadSize;
    if (getWnd() < bytesToSend) {
        Log(INFO, std::format("no send attempt - wnd (cwnd={}, rwnd={}) < bytesToSend ({})", cwnd, rwnd, bytesToSend));
        return -1;
    }

    int64_t size = payloadSize;
    if (hdr.SYN || hdr.FIN) {
        size += 1;
    }

    // write header + payload to intermediate buffer
    std::memcpy(preSendBuffer, &hdr, sizeof(hdr));
    if (payloadSize > 0) {
        readFromBuffer(nxtPos, preSendBuffer + sizeof(hdr), payloadSize);
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

    nxt += size;
    nxtPos = (nxt - iss) % capacity;
    if (nxt > write_) {
        // SYN/FIN add bytes the user never wrote - in this case, drag write_/writePos back up
        write_ = nxt;
        writePos = (write_ - iss) % capacity;
    }

    // queue in-flight segment
    SendSegment seg;
    seg.hdr = hdr;
    seg.seqNum = hdr.seqNum;
    seg.size = size;

    inFlightSegments.push_back(seg);

    // queue rto if: a) no active RTO, and b) segment consumes seqnums
    if (!rto.active && size > 0) {
        queueRto(seg);
    }

    return bytesSent;
}

int64_t SendStream::retransmitSegment(SendSegment &seg) {
    if (getWnd() < MSS) {
        Log(INFO, std::format("no send attempt - wnd < MSS (cwnd={}, rwnd={})", cwnd, rwnd));
        return -1;
    }

    // wire payload = seqnum span minus any wasted SYN/FIN byte
    int64_t payloadSize = seg.size;
    if (seg.hdr.SYN || seg.hdr.FIN) {
        payloadSize -= 1;
    }

    std::memcpy(preSendBuffer, &seg.hdr, sizeof(seg.hdr));
    if (payloadSize > 0) {
        // retransmit is always the oldest in-flight segment, whose payload starts at unaPos
        readFromBuffer(unaPos, preSendBuffer + sizeof(seg.hdr), payloadSize);
    }

    int bytesToSend = sizeof(seg.hdr) + payloadSize;
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

    return bytesSent;
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
    return write_ - nxt;
}

int64_t SendStream::freeSpaceBytes() {
    return capacity - 1 - (write_ - una); // reserve 1-byte, so a full buffer is distinguishable from empty
}

int64_t SendStream::generateIss() {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int64_t> dist(0, MAX_ISS);
    return dist(generator);
}

bool SendStream::bufferStateValid() {
    if (unaPos != (una - iss) % capacity) {
        Log(ERROR, std::format("unaPos ({}) is incorrect buffer position of una ({})", unaPos, una));
        return false;
    }
    if (nxtPos != (nxt - iss) % capacity) {
        Log(ERROR, std::format("nxtPos ({}) is incorrect buffer position of nxt ({})", nxtPos, nxt));
        return false;
    }
    if (writePos != (write_ - iss) % capacity) {
        Log(ERROR, std::format("writePos ({}) is incorrect buffer position of write ({})", writePos, write_));
        return false;
    }
    if (!inFlightSegments.empty() && inFlightSegments.front().seqNum != una) {
        Log(ERROR, std::format("first in-flight segment seqNum ({}) != una ({})", inFlightSegments.front().seqNum, una));
        return false;
    }
    return true;
}