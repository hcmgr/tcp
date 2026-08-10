#include "send.hpp"
#include "utils.hpp"

send_stream::send_stream(uint64_t capacity) {
    iss_ = rng::generate_iss();
    una_ = iss_;
    nxt_ = iss_;
    fin_ = 0;

    una_pos_ = 0;
    nxt_pos_ = 0;
    wr_pos_ = 0;

    buffer_ = new ring_buffer(capacity);

    cong_ = new congestion_controller();
}

send_stream::~send_stream() {

}

void send_stream::send_syn() {

}

void send_stream::send_syn_ack() {

}

void send_stream::send_ack() {

}

void send_stream::send_fin() {

}

void send_stream::send_rst() {

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

int64_t send_stream::on_ack(uint64_t acknum) {
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
    // package up ready bytes into segments

}

void send_stream::retransmit_oldest_segment() {

}

void send_stream::on_rto() {

}

void send_stream::on_triple_dup_ack() {

}