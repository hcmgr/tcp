#include <cstring>
#include <algorithm>

#include "engine.hpp"

RecvStream::RecvStream(Connection *conn)
    : state(RecvStreamState::CLOSED), connRef(conn) {}

RecvStream::~RecvStream() {
    free(buffer);
    receivedSegments.clear();
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
    nxt = irs + 1; // have consumed irs byte => next byte we expect is irs + 1
    read_ = irs + 1;

    // physical positions are 1:1 with the logical seqnums. the peer's SYN (irs) wastes a byte at
    // position 0, so the first data byte (irs + 1) maps to position 1.
    readPos = (read_ - irs) % capacity;
    nxtPos = (nxt - irs) % capacity;

    cumSegmentSize = 0;

    state = RecvStreamState::INITIALISED;
}

void RecvStream::read(int64_t n, uint8_t *outBuffer) {
    int64_t bytesAvail = readyToReadBytes();
    if (bytesAvail < n) {
        //
        // Insufficient data to read.
        //
        // TODO - put to sleep, wake on available data
        //
        Log(INFO, std::format("insufficient data for read() - {} bytes avail, {} bytes to read - sleep until sufficient data available", bytesAvail, n));
        return;
    }

    readFromBuffer(readPos, outBuffer, n);
    read_ += n;
    readPos = (read_ - irs) % capacity;
    return;
}

void RecvStream::receiveSegment(Header &hdr, int64_t payloadSize, uint8_t *payloadPtr) {
    // seqnum span = payload bytes, plus 1 wasted byte per SYN/FIN
    int64_t size = payloadSize;
    if (hdr.SYN || hdr.FIN) {
        size += 1;
    }
    if (size == 0) {
        // consumes no sequence space - nothing to receive
        return;
    }

    //
    // received segments only valid in seqnum range: [nxt, read)
    //
    int64_t segL = hdr.seqNum;
    int64_t segR = hdr.seqNum + size - 1; // inclusive
    if (segL < nxt || read_ <= segR){
        Log(ERROR, std::format("out of range"));
        return;
    }

    RecvSegment seg;
    seg.seqNum = hdr.seqNum;
    seg.size = size;

    // payload buffer position - data sits after any SYN byte (positions are 1:1 with seqnums)
    int64_t payloadSeqNum = hdr.seqNum;
    if (hdr.SYN || hdr.FIN) {
        payloadSeqNum += 1;
    }
    int64_t payloadPos = (payloadSeqNum - irs) % capacity;

    //
    // place segment at correct location in our 'receivedSegments' deque (can be out-of-order)
    //
    bool placed = false;
    auto it = receivedSegments.begin();
    while (it != receivedSegments.end()) {
        RecvSegment &curr = *it;
        int64_t currL = curr.seqNum;
        int64_t currR = curr.seqNum + curr.size - 1; // inclusive
        if (segR <= currL) {
            // seg somewhere before curr - place it
            receivedSegments.insert(it, seg);
            cumSegmentSize += seg.size;
            writeToBuffer(payloadPos, payloadPtr, payloadSize);
            placed = true;
            break;
        } else if (currR < segL) {
            // seg somewhere after curr - wait to place
            it++;
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
        writeToBuffer(payloadPos, payloadPtr, payloadSize);
    }

    //
    // advance nxt as far as segments are contiguous
    //
    while (!receivedSegments.empty()) {
        RecvSegment &curr = receivedSegments.front();
        if (nxt == curr.seqNum) {
            nxt += curr.size;
            nxtPos = (nxt - irs) % capacity;
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

void RecvStream::writeToBuffer(int64_t pos, uint8_t *src, int64_t n) {
    int64_t firstChunk = std::min(n, capacity - pos);
    std::memcpy(buffer + pos, src, firstChunk);
    if (n > firstChunk) {
        std::memcpy(buffer, src + firstChunk, n - firstChunk);
    }
}

void RecvStream::readFromBuffer(int64_t pos, uint8_t *dest, int64_t n) {
    int64_t firstChunk = std::min(n, capacity - pos);
    std::memcpy(dest, buffer + pos, firstChunk);
    if (n > firstChunk) {
        std::memcpy(dest + firstChunk, buffer, n - firstChunk);
    }
}

//
// For now:
//      - send acks immediately on receipt of new data
// Later, implement delayed acks
//      - queue ack for X ms, waiting to piggyback it onto a sending segment (or another ack)
//
void RecvStream::attemptAck() {
    Engine::getInstance().sendAck(connRef);
}

//
// 'Ready-to-read' space is in front of readPos before nxtPos.
//
int64_t RecvStream::readyToReadBytes() {
    return ((nxtPos + capacity) - readPos) % capacity;
}

//
// Occupied buffer = contiguous-unread (readPos -> nxtPos) + out-of-order payload bytes already
// placed ahead of nxtPos (cumSegmentSize). Reserve one byte so a full buffer is distinguishable
// from an empty one.
//
int64_t RecvStream::freeSpaceBytes() {
    int64_t contiguousUnread = ((nxtPos + capacity) - readPos) % capacity;
    return capacity - 1 - contiguousUnread - cumSegmentSize;
}

bool RecvStream::bufferStateValid() {
    if (nxtPos != (nxt - irs) % capacity) {
        Log(ERROR, std::format("nxtPos ({}) is incorrect buffer position of nxt ({})", nxtPos, nxt));
        return false;
    }
    if (readPos != (read_ - irs) % capacity) {
        Log(ERROR, std::format("readPos ({}) is incorrect buffer position of read ({})", readPos, read_));
        return false;
    }
    if (!receivedSegments.empty() && receivedSegments.front().seqNum == nxt) {
        Log(ERROR, std::format("front segment's seqNum should not be == nxt, nxt should have advanced to first non-contiguous segment"));
        return false;
    }
    return true;
}
