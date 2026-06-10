#include <cstring>
#include <algorithm>

#include "engine.hpp"

RecvStream::RecvStream()
    : state(RecvStreamState::CLOSED) {}

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

    readPos = (read_ - irs) % capacity;
    nxtPos = (nxt - irs) % capacity;

    state = RecvStreamState::INITIALISED;
}

void RecvStream::read(int64_t n, uint8_t *outBuffer) {
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
    read_ += n;
    readPos += n;
    return;
}

void RecvStream::receiveSegment(Header &hdr, int64_t n, uint8_t *payloadPtr) {
    int64_t seqNumSize = n;
    if (hdr.SYN || hdr.FIN) {
        seqNumSize += 1;
    }
    if (seqNumSize == 0) {
        // consumes no sequence space - nothing to receive
        return;
    }

    //
    // received segments only valid in seqnum range: [nxt, read)
    //
    int64_t segL = hdr.seqNum;
    int64_t segR = hdr.seqNum + seqNumSize - 1; // inclusive
    if (segL < nxt || read_ <= segR){
        Log(ERROR, std::format("out of range"));
        return;
    }

    RecvSegment seg;
    seg.seqNum = hdr.seqNum;
    seg.payloadSize = n;
    seg.seqNumSize = seqNumSize;

    // payload buffer position - data sits after any SYN byte
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
    while (!receivedSegments.empty()) {
        RecvSegment &curr = *it;
        int64_t currL = curr.seqNum;
        int64_t currR = curr.seqNum + curr.seqNumSize - 1; // inclusive
        if (segR <= currL) {
            // seg somewhere before curr - place it
            receivedSegments.insert(it, seg);
            cumSegmentSize += seg.seqNumSize;
            writeToBuffer(payloadPos, payloadPtr, n);
            placed = true;
            break;
        } else if (currR < segL) {
            // seg somewhere after curr - wait to place
            continue;
        } else {
            // invalid
            Log(ERROR, std::format("invalid range of new segment: seqNum={}, seqNumSize={}", seg.seqNum, seg.seqNumSize));
            return;
        }
    }
    if (!placed) {
        // not placed yet - place at end
        receivedSegments.push_back(seg);
        cumSegmentSize += seg.seqNumSize;
        writeToBuffer(payloadPos, payloadPtr, n);
    }

    //
    // advance nxt as far as segments are contiguous
    //
    while (!receivedSegments.empty()) {
        RecvSegment &curr = receivedSegments.front();
        if (nxt == curr.seqNum) {
            nxt += curr.seqNumSize;
            nxtPos = (nxtPos + curr.seqNumSize) % capacity;
            cumSegmentSize -= curr.seqNumSize;
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
// Free space is in front of nxtPos before readPos.
// Segments are placed out-of-order in front of nxtPos, but nxtPos only moves once
// section in front of it is contiguous. So, reduce free space by `cumSegmentSize`.
//
int64_t RecvStream::freeSpaceBytes() {
    return (((readPos + capacity) - nxtPos) % capacity) - cumSegmentSize;
}

bool RecvStream::bufferStateValid() {
    if (nxtPos != ((nxt - irs) % capacity)) {
        Log(ERROR, std::format("nxtPos ({}) is incorrect buffer position of nxt ({})", nxtPos, nxt));
        return false;
    }
    if (readPos != ((read_ - irs) % capacity)) {
        Log(ERROR, std::format("readPos ({}) is incorrect buffer position of read ({})", readPos, read_));
        return false;
    }
    if (!receivedSegments.empty() && receivedSegments.front().seqNum == nxt) {
        Log(ERROR, std::format("front segment's seqNum should not be == nxt, nxt should have advanced to first non-contiguous segment"));
        return false;
    }
    return true;
}
