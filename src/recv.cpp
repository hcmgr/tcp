#include <cstring>
#include <algorithm>

#include "engine.hpp"

RecvStream::RecvStream(Connection *conn)
    : state(RecvStreamState::CLOSED), connRef(conn) {}

RecvStream::~RecvStream() {
    free(buffer);
    pendingSegments.clear();
    state = RecvStreamState::CLOSED;
}

int64_t RecvStream::getNxt() { return nxt; }
int64_t RecvStream::getWnd() { return freeSpaceBytes(); }

void RecvStream::init(int64_t _irs) {
    capacity = RECV_BUFFER_CAPACITY;
    buffer = (uint8_t*)malloc(capacity);
    if (buffer == nullptr) {
        Log(ERROR, std::format("couldn't allocate recv buffer - {} bytes", capacity));
        return;
    }

    irs = _irs;
    nxt = irs + 1; // consumed IRS from SYN already
    read_ = irs + 1; // consumed IRS from SYN already

    readPos = 0;
    nxtPos = 0;

    cumPendingSegmentsSize = 0;

    state = RecvStreamState::INITIALISED;
}

int64_t RecvStream::read(int64_t n, uint8_t *outBuffer) {
    int64_t bytesAvail = readyToReadBytes();
    if (bytesAvail < n) {
        // insufficient data to read
        Log(INFO, std::format("insufficient data for read() - {} bytes avail, {} bytes to read", bytesAvail, n));
        return 0;
    }

    readFromBuffer(readPos, outBuffer, n);
    read_ += n;
    readPos = (readPos + n) % capacity;

    return n;
}

void RecvStream::receiveSegment(RecvSegment &seg, uint8_t *payloadPtr) {
    //
    // ensure within range: [nxt, nxt + wnd)
    //
    if (seg.hdr.seqNum < nxt) {
        Log(ERROR, std::format("dropping already-received segment (seqNum {} < nxt {}): {}", seg.hdr.seqNum, nxt));
        return;
    }
    int64_t wndEnd = nxt + freeSpaceBytes(); // first seqnum past the receive window
    if (seg.hdr.seqNum + seg.size - 1 >= wndEnd) {
        Log(ERROR, std::format("received segment outside window - seqNum + size - 1 ({}) >= wndEnd ({})", seg.hdr.seqNum + seg.size - 1, wndEnd));
        return;
    }

    //
    // place segment at correct location in our 'receivedSegments' deque (can be out-of-order)
    //

    int64_t segL = seg.hdr.seqNum;
    int64_t segR = seg.hdr.seqNum + seg.size - 1; // inclusive
    if (seg.hdr.SYN) {
        segR += 1;
    } else if (seg.hdr.FIN) {
        segR += 1;
    }

    bool placed = false;
    auto it = pendingSegments.begin();
    while (it != pendingSegments.end()) {
        RecvSegment &curr = *it;
        int64_t currL = curr.hdr.seqNum;
        int64_t currR = curr.hdr.seqNum + curr.size - 1; // inclusive
        if (curr.hdr.FIN) {
            currR += 1;
        }

        if (segR <= currL) {
            // seg somewhere before curr - place it
            pendingSegments.insert(it, seg);
            cumPendingSegmentsSize += seg.size;
            int64_t pos = (nxtPos + (seg.hdr.seqNum - nxt)) % capacity;
            writeToBuffer(pos, payloadPtr, seg.size);
            placed = true;
            break;
        } else if (currR < segL) {
            // seg somewhere after curr - wait to place
            it++;
            continue;
        } else {
            // invalid
            Log(ERROR, std::format("invalid range of new segment: {}", seg.toString()));
            return;
        }
    }
    if (!placed) {
        // not placed yet - must be at end
        pendingSegments.push_back(seg);
        cumPendingSegmentsSize += seg.size;
        int64_t pos = (nxtPos + (seg.hdr.seqNum - nxt)) % capacity;
        writeToBuffer(pos, payloadPtr, seg.size);
    }

    //
    // advance nxt as far as segments are contiguous
    //
    while (!pendingSegments.empty()) {
        RecvSegment &curr = pendingSegments.front();
        if (nxt == curr.hdr.seqNum) {
            nxt += curr.size;
            if (curr.hdr.SYN) {
                nxt += 1;
            } else if (curr.hdr.FIN) {
                nxt += 1;
            }
            nxtPos = (nxtPos + curr.size) % capacity;
            cumPendingSegmentsSize -= curr.size;
            pendingSegments.pop_front();
        } else {
            break;
        }
    }
}

void RecvStream::writeToBuffer(int64_t pos, uint8_t *src, int64_t n) {
    int64_t firstChunk = std::min<int64_t>(n, capacity - pos);
    std::memcpy(buffer + pos, src, firstChunk);
    if (n > firstChunk) {
        std::memcpy(buffer, src + firstChunk, n - firstChunk);
    }
}

void RecvStream::readFromBuffer(int64_t pos, uint8_t *dest, int64_t n) {
    int64_t firstChunk = std::min<int64_t>(n, capacity - pos);
    std::memcpy(dest, buffer + pos, firstChunk);
    if (n > firstChunk) {
        std::memcpy(dest + firstChunk, buffer, n - firstChunk);
    }
}

void RecvStream::attemptAck() {
    //
    // For now, send acks immediately on receipt of new data
    // Later, implement delayed acks:
    //      - queue ack for X ms, waiting to piggyback it onto a sending segment (or another ack)
    //
    Engine::getInstance().sendAck(connRef);
}

int64_t RecvStream::readyToReadBytes() {
    return ((nxtPos + capacity) - readPos) % capacity;
}

int64_t RecvStream::freeSpaceBytes() {
    int64_t ready = readyToReadBytes();

    // reduce free-space by 1, so we can distinguish between empty and full
    return capacity - 1 - ready - cumPendingSegmentsSize;
}