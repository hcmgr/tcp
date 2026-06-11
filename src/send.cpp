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
    nxt = iss; // first byte to send is ISS

    unaPos = 0;
    nxtPos = 0;
    writePos = 0;

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
    writePos = (writePos + n) % capacity;

    // new data available - send ready segments
    sendReadySegments();
}

bool SendStream::onAck(int64_t ackNum) {
    bool tripleDupAck = false;
    if (ackNum == lastAcks[0] && ackNum == lastAcks[1]) {
        tripleDupAck = true;
        std::swap(lastAcks[0], lastAcks[1]);
        lastAcks[1] = ackNum;
    }

    //
    // ack meaningful if its in range [una, nxt), i.e. if its ack'ing un-ack'd bytes
    //
    if (ackNum < una) {
        Log(ERROR, std::format("received ackNum ({}) < una ({}) - invalid", ackNum, una));
        return false;
    }
    if (ackNum >= nxt) {
        Log(ERROR, std::format("received ackNum ({}) > nxt ({}) - invalid", ackNum, nxt));
        return false;
    }

    // duplicate ack
    if (ackNum == una) {
        if (tripleDupAck) {
            // triple duplicate ack - fast retransmit
            int64_t sent = retransmitOldestSegment();
            if (sent == -1) {
                return false;
            }
        } else {
            // normal duplicate ack - do nothing
        }
        return true;
    }

    // advance una, removing inFlightSegments as necessary
    while (!inFlightSegments.empty()) {
        SendSegment &seg = inFlightSegments.front();
        int64_t seqNumSize = seg.size;
        if (seg.hdr.SYN) {
            seqNumSize += 1;
        } else if (seg.hdr.FIN) {
            seqNumSize += 1;
        }

        if (seg.hdr.seqNum + seqNumSize <= ackNum) {
            //
            // segment fully ack'd - remove segment
            //
            una += seqNumSize;
            unaPos += seg.size;

            inFlightSegments.pop_front();
        }
        else if (seg.hdr.seqNum < ackNum && ackNum < seg.hdr.seqNum + seqNumSize) {
            //
            // segment partially ack'd - update segment (rarely happens, most acks will be full)
            //
            int64_t ackedSeqNums = ackNum - seqNumSize;
            int64_t ackedBytes = ackNum - seg.size;

            una += ackedSeqNums;
            unaPos = (unaPos + ackedBytes) % capacity;

            seg.hdr.seqNum += ackedSeqNums;
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
    SendSegment &oldestSeg = inFlightSegments.front();
    if (!(oldestSeg.hdr.seqNum == una)) {
        Log(ERROR, std::format("oldest segment seqNum ({}) != una ({}) - invalid", oldestSeg.hdr.seqNum, una));
        return;
    }
    if (!rtoSeg.equals(oldestSeg)) {
        Log(ERROR, std::format("rto segment does not match oldest segment - oldest={}, rto={}", oldestSeg.toString(), rtoSeg.toString()));
        return false;
    }

    // retransmit
    int64_t bytesSent = retransmitOldestSegment();
    if (bytesSent == -1) {
        return false;
    }

    // restart rto for same segment
    bool res = cancelRto();
    if (!res) {
        return false;
    }
    res = queueRto(oldestSeg);
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

        SendSegment seg;
        seg.hdr = hdr;
        seg.size = MSS;

        int64_t bytesSent = sendNextSegment(seg);
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
    // Also, perhaps pin it to our window size - i.e. account for window size at this point,
    // not just in sendNextSegment().
    //
}

int64_t SendStream::sendNextSegment(SendSegment &seg) {
    if (seg.hdr.seqNum != nxt) {
        Log(ERROR, std::format("sendNextSegment invalid - seqNum {} != nxt, i.e. segment is not next segment", seg.hdr.seqNum, nxt));
        return;
    }
    if (seg.size > MSS) {
        Log(ERROR, std::format("cannot send payload size > 1 MSS - {}", seg.size));
        return -1;
    }
    int bytesToSend = sizeof(seg.hdr) + seg.size;

    // check window
    int64_t sendWnd = std::min<int64_t>(cong.getCwnd(), rwnd);
    if (seg.size > sendWnd) {
        Log(INFO,
            std::format(
                "payload size ({}) > wnd ({}) == min(cwnd ({}), rwnd ({})), wait for larger window size", 
                seg.size, sendWnd, cong.getCwnd(), rwnd));
        // not an error - just wait for larger window size
        return 0;
    }

    // write header + payload to intermediate buffer
    std::memcpy(preSendBuffer, &seg.hdr, sizeof(seg.hdr));
    if (seg.size > 0) {
        readFromBuffer(nxtPos, preSendBuffer + sizeof(seg.hdr), seg.size);
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
    nxt += seg.size;
    if (seg.hdr.SYN) {
        nxt += 1;
    } else if (seg.hdr.FIN) {
        nxt += 1;
    }
    nxtPos = (nxtPos + seg.size) % capacity;

    // queue in-flight segment
    inFlightSegments.push_back(seg);

    // queue rto if: no current rto, and segment consumes seqnum space
    bool consumesSeqNum = seg.hdr.SYN || seg.hdr.FIN || seg.size > 0;
    if (!rto.active && consumesSeqNum) {
        queueRto(seg);
    }

    return bytesSent;
}

int64_t SendStream::retransmitOldestSegment() {
    SendSegment &seg = inFlightSegments.front();
    if (seg.hdr.seqNum != una) {
        Log(ERROR, std::format("retransmitSegment invalid - seqNum {} != una {}, i.e. segment is not oldest un-ack'd segment", seg.hdr.seqNum, una));
        return -1;
    }

    // check window
    int64_t sendWnd = std::min<int64_t>(cong.getCwnd(), rwnd);
    if (seg.size > sendWnd) {
        Log(INFO,
            std::format(
                "payload size ({}) > wnd ({}) == min(cwnd ({}), rwnd ({})), wait for larger window size", 
                seg.size, sendWnd, cong.getCwnd(), rwnd));
        // not an error - just wait for larger window size
        return 0;
    }

    std::memcpy(preSendBuffer, &seg.hdr, sizeof(seg.hdr));
    if (seg.size > 0) {
        readFromBuffer(unaPos, preSendBuffer + sizeof(seg.hdr), seg.size);
    }

    int bytesToSend = sizeof(seg.hdr) + seg.size;
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
    int64_t firstChunk = std::min<int64_t>(n, capacity - pos);
    std::memcpy(buffer + pos, src, firstChunk);
    if (n > firstChunk) {
        std::memcpy(buffer, src + firstChunk, n - firstChunk);
    }
}

void SendStream::readFromBuffer(int64_t pos, uint8_t *dest, int64_t n) {
    int64_t firstChunk = std::min<int64_t>(n, capacity - pos);
    std::memcpy(dest, buffer + pos, firstChunk);
    if (n > firstChunk) {
        std::memcpy(dest + firstChunk, buffer, n - firstChunk);
    }
}

int64_t SendStream::readyToSendBytes() {
    return ((writePos + capacity) - nxtPos) % capacity;
}

int64_t SendStream::freeSpaceBytes() {
    return 0;
}

int64_t SendStream::generateIss() {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int64_t> dist(0, MAX_ISS);
    return dist(generator);
}