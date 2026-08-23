#include "send.hpp"
#include "utils.hpp"

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
}

send_stream::~send_stream() {

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
    return n;
}

int64_t send_stream::on_ack_recv(uint64_t acknum) {
    if (acknum < una_) {
        // old ack - ignore
        return 0;
    }

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

    // TODO - protect against ridiculous number being sent that messses with our una_pos_

    una_ = acknum;
    una_pos_ = inc(una_pos_, acknum - una_);

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

void send_stream::send_ready_bytes() {
    //
    // Send all 1-MSS segments we have avail + remainder.
    //
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
            // send_stream will be torndown by owning connection for bad send()
            return;
        }

        // advance nxt
        nxt_ += len;
        inc(nxt_pos_, len);
    }
}

void send_stream::retransmit_oldest_segment() {

}

void send_stream::on_rto() {

}

void send_stream::on_triple_dup_ack() {

}

void send_stream::on_delayed_ack_timeout() {

}

void send_stream::on_delayed_send_timeout() {

}