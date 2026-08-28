#include "send.hpp"
#include "utils.hpp"
#include "event_loop.hpp"
#include "manager.hpp"

send_stream::send_stream(uint64_t capacity, send_segment_cb send_segment_cb) {
    iss_ = rng::generate_iss();
    una_ = iss_;
    nxt_ = iss_;
    fin_ = 0;

    una_pos_ = 0;
    nxt_pos_ = 0;
    wr_pos_ = 0;

    buffer_ = new ring_buffer(capacity);

    cong_ = new congestion_controller();

    send_segment_cb_ = send_segment_cb;

    retransmission_timeout_ = new timeout_handler(
        RTO_TIMEOUT_MS,
        libevent_on_retransmission_timeout,
        this);
}

send_stream::~send_stream() {
    delete buffer_;
    delete cong_;
    delete retransmission_timeout_;
}

int64_t send_stream::on_syn_sent() {
    nxt_ += 1;

    if (!(una_ == iss_  && nxt_ == iss_ + 1)) {
        Log(level::ERROR, 
            std::format(
                "bad send state reached after on_syn_sent: should have una == iss and nxt == iss + 1 - {}", 
                to_string()));
        return -1;
    }

    return 0;
}

int64_t send_stream::write(uint64_t n, uint8_t *src_buffer) {
    if (n == 0) return 0;

    if (src_buffer == nullptr) {
        Log(level::ERROR, "write() src_buffer is null");
        return -1;
    }

    uint64_t free_space = free_space_bytes();
    if (free_space < n) {
        Log(level::ERROR, std::format("insufficient free space for write(): {} < {}", free_space, n));
        return -1;
    }

    buffer_->write(wr_pos_, src_buffer, n);

    // write made new data available => try send ready bytes
    auto res = send_ready_bytes();
    if (res < 0) {
        return -1;
    }

    return n;
}

int64_t send_stream::on_ack_recv(uint64_t acknum) {
    if (acknum < una_) {
        // old ack - ignore
        return 0;
    }

    // todo - protect against ridiculously large acknum

    //
    // check for triple-dup-ack
    //
    auto &last_acks = dup_ack_.last_acks;
    if (last_acks[0] == last_acks[1] && last_acks[1] == acknum) {
        if (acknum > una_) {
            //
            // Fatal case.
            //
            // If new acknum equal to last 2 acks, acknum has to == una.
            // Otherwise, the previous acks would have just advanced una already.
            //
            // Fail here, and owning connection should trigger teardown.
            //
            Log(level::ERROR, std::format("triple dup-ack detected, yet acknum ({}) != una ({}) - teardown", acknum, una_));
            return -1;
        }

        // triple-dup-ack detected - fast retransmit oldest segment
        auto res = retransmit_oldest_segment();
        if (res < 0) {
            return -1;
        }

        cong_->on_triple_dup_ack();
    } 
    else {
        std::swap(last_acks[0], last_acks[1]);
        last_acks[1] = acknum;

        cong_->on_ack();
    }

    //
    // advance una, popping segments of our in-flight-queue as necessary
    //
    while (!in_flight_segments_.empty()) {
        segment &seg = in_flight_segments_.front();
        if (acknum < seg.seqnum) {
            // all now-acked in-flight-segments removed - stop
            break;
        }
        else if (seg.seqnum <= acknum && acknum <= seg.seqnum + seg.payload_size) {
            // segment partially-acked - update
            uint64_t diff = acknum - seg.seqnum;
            seg.seqnum += diff;
            seg.payload_size -= diff;
            seg.payload_pos = inc(seg.payload_pos, diff);
        } 
        else {
            // fully acked segment - remove
            in_flight_segments_.pop_front();
        }
    }

    una_ = acknum;
    una_pos_ = inc(una_pos_, acknum - una_);
    
    // ack potentially made new data avail => try send ready bytes
    auto res = send_ready_bytes();
    if (res < 0) {
        return -1;
    }

    return 0;
}

uint64_t send_stream::ready_bytes() {
    uint64_t capacity = buffer_->capacity();
    return ((wr_pos_ + capacity) - nxt_pos_) % capacity;
}

uint64_t send_stream::free_space_bytes() {
    uint64_t capacity = buffer_->capacity();
    return ((una_pos_ + capacity) - wr_pos_) % capacity;
}

int64_t send_stream::send_ready_bytes() {
    //
    // Send all 1-MSS segments we have avail + remainder.
    // For now, send remainder immediately.
    // In future:
    //      If sent (full segments already sent), send immediately.
    //      Otherwise (< 1-MSS avail), queue a delayed send.
    //
    uint64_t ready = 0;
    while ((ready = ready_bytes()) > 0) {
        uint32_t seqnum = nxt_;
        uint16_t flags = ack_mask;
        uint64_t len = ready >= MSS ? MSS : ready;
        uint8_t buf[len];
        buffer_->read(nxt_pos_, buf, len);

        int64_t sent_bytes = send_segment_cb_(seqnum, flags, buf, len);
        if (sent_bytes != MSS) {
            return -1;
        }

        // advance nxt
        nxt_ += len;
        inc(nxt_pos_, len);
    }

    return 0;
}

int64_t send_stream::retransmit_oldest_segment() {
    if (in_flight_segments_.empty()) {
        Log(level::ERROR, "retransmit_oldest_segment() invoked with empty in-flight-segments queue");
        return -1;
    }

    segment &seg = in_flight_segments_.front();
    uint32_t seqnum = seg.seqnum;
    uint16_t flags = ack_mask;
    uint64_t len = seg.payload_size;
    uint8_t buf[len];
    buffer_->read(nxt_pos_, buf, len);

    int64_t sent_bytes = send_segment_cb_(seqnum, flags, buf, len);
    if (sent_bytes != len) {
        return -1;
    }
    return sent_bytes;
}

void send_stream::on_retransmission_timeout() {
    cong_->on_rto();
}