#include <format>

#include "recv.hpp"
#include "utils.hpp"
#include "define.hpp"

recv_stream::recv_stream(uint64_t capacity) {
    irs_ = 0;
    nxt_ = 0;
    fin_ = 0;

    nxt_pos_ = 0;
    rd_pos_ = 0;

    buffer_ = new ring_buffer(capacity);

    state_ = state::SYN_WAITING;
}

recv_stream::~recv_stream() {
    pending_segments_.clear();
}

int64_t recv_stream::recv_segment(uint64_t seqnum, uint8_t *payload_ptr, uint64_t payload_size) {
    if (state_ != state::ESTABLISHED) {
        Log(level::ERROR, "recv_segment() not in ESTABLISHED state - dropping");
        return -1;
    }

    if (seqnum < nxt_ && seqnum + payload_size <= nxt_) {
        // segment already received - silent drop
        return 0;
    }

    if (seqnum < nxt_ && seqnum + payload_size > nxt_) {
        // segment already partially-received - trim start so we only take new bytes
        uint64_t already_recveived = nxt_ - seqnum;
        seqnum = nxt_;
        payload_size -= already_recveived;
        payload_ptr += already_recveived;
    }

    uint64_t free_space = free_space_bytes();
    if (payload_size > free_space) {
        Log(level::ERROR, std::format("insufficient free space for segment - {} > {}", payload_size, free_space));
        return -1;
    }

    // write payload out to buffer
    uint64_t pos = inc(nxt_pos_, seqnum - nxt_);
    buffer_->write(pos, payload_ptr, payload_size);

    segment seg;
    seg.seqnum = seqnum;
    seg.payload_size = payload_size;
    seg.payload_pos = pos;

    //
    // insert segment into correct location in our pending segments queue
    //
    auto it = pending_segments_.begin();
    while (it != pending_segments_.end()) {
        if (seg.seqnum < it->seqnum) {
            // seg belongs directly behind `it` - stop
            break;
        }
        it++;
    }

    if (it != pending_segments_.end() && seg.seqnum + seg.payload_size >= it->seqnum) {
        // new segment overlaps with one in front - trim end so it fits contiguous (resting segment wins)
        uint64_t overlap = (seg.seqnum + seg.payload_size) - it->seqnum;
        seg.payload_size -= overlap;
    }
    pending_segments_.insert(it, seg);

    //
    // advance nxt past its contiguous segments
    //
    while (!pending_segments_.empty()) {
        auto it = pending_segments_.begin();
        if (nxt_ < it->seqnum) {
            break;
        } 
        else if (nxt_ == it->seqnum) {
            nxt_ += it->payload_size;
            nxt_pos_ = inc(nxt_pos_, it->payload_size);
            pending_segments_.pop_front();
        } 
        else {
            Log(level::ERROR, std::format("nxt_ ({}) > pending segment seqnum ({})", nxt_, it->seqnum));
            return -1;
        }
    }

    return seg.payload_size;
}

int64_t recv_stream::read(uint64_t n, uint8_t *dest_buffer) {
    if (n == 0) {
        return 0;
    }
    if (dest_buffer == nullptr) {
        Log(level::ERROR, "read() dest_buffer == nullptr");
        return -1;
    }

    uint64_t ready_to_read = ready_bytes();
    if (ready_to_read < n) {
        Log(level::ERROR, std::format("read() ready-to-read-bytes ({}) < n ({})", ready_to_read, n));
        return -1;
    }
    
    buffer_->read(rd_pos_, dest_buffer, n);
    rd_pos_ = inc(rd_pos_, n);
    return n;
}

void recv_stream::on_syn(uint64_t irs) {
    if (state_ != state::SYN_WAITING) {
        Log(level::ERROR, "on_syn() not in SYN_WAITING state - dropping");
        return;
    }
    irs_ = irs;
    nxt_ = irs_ + 1;
    state_ = state::ESTABLISHED;
}

void recv_stream::on_fin(uint64_t fin) {
    if (state_ != state::ESTABLISHED) {
        Log(level::ERROR, "on_fin() not in ESTABLISHED state - dropping");
        return;
    }
    fin_ = fin;
    state_ = state::FINISHED;
}

int64_t recv_stream::free_space_bytes() {
    // Bytes from furthest_pos -> rd_pos_, where furthest_pos is the 
    // buffer position of our furthest pending segment.
    //
    // For example, suppose segs 2 and 3 have arrived out-of-order, and we're waiting for seg 1.
    // Our recv space looks like:
    //
    //          |-----[free for seg1]-----|---seg2---|--------seg3-------|---------------|
    //       nxt_pos                                                furthest_pos       rd_pos
    //
    // So, we report our free space as furthest_pos -> rd_pos. Note that we still have free space 
    // for seg1. Not reporting this as free space is a design choice. Motivation:
    //
    //      This out-of-order scenario crops up when something has gone wrong, e.g. seg1
    //      dropped for some reason. As such, as triple-dup-ack / rto will trigger,
    //      and seg1 will be re-transmitted. If we keep receiving other segments (4, 5, etc.)
    //      our window will continue to shrink (rd_pos progressing, furthest_pos not).
    //      On receipt of seg1, nxt_pos will advance past segs1-3, furthest_pos can advance,
    //      and our window will grow again.
    //
    //      There is an edge case where the window shrinks to the point where a valid seg1
    //      is no longer permitted (e.g. seg1 500 bytes > furthest_pos->rd_pos == 450 bytes).
    //      For a large enough buffer (we use ~64K), reaching this point implies some
    //      catastrophic failure, and we're probably better off hitting a connection.reset().
    // 
    uint64_t furthest_pos = nxt_pos_;
    if (!pending_segments_.empty()) {
        const segment &back = pending_segments_.back();
        furthest_pos = inc(furthest_pos, ((back.seqnum + back.payload_size) - nxt_));
    }
    uint64_t capacity = buffer_->capacity();
    return (((rd_pos_ + capacity) - furthest_pos) % capacity) - 1;
}

int64_t recv_stream::ready_bytes() {
    uint64_t capacity = buffer_->capacity();
    return (((nxt_pos_ + capacity) - rd_pos_) % capacity);
}