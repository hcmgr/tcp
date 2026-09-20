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

    buffer_ = std::make_unique<ring_buffer>(capacity);

    cong_ = std::make_unique<congestion_controller>();

    peer_recv_window_ = 0;

    send_segment_cb_ = send_segment_cb;

    retransmission_timeout_ = std::make_unique<timeout_handler>(
        RTO_TIMEOUT_MS,
        libevent_on_retransmission_timeout,
        this);

    state_ = state::ESTABLISHED;
}

send_stream::~send_stream() {}

int64_t send_stream::write(uint64_t n, uint8_t *src_buffer) {
    if (state_ != state::ESTABLISHED) {
        return -1;
    }

    if (n == 0) return 0;
    if (src_buffer == nullptr) {
        Log(level::ERROR, "write() src_buffer is null");
        return -1;
    }

    uint64_t free_space = get_num_free_space_bytes();
    if (free_space < n) {
        Log(level::ERROR, std::format("insufficient free space for write(): {} < {}", free_space, n));
        return -1;
    }

    buffer_->write(wr_pos_, src_buffer, n);
    wr_pos_ = inc(wr_pos_, n);

    // write made new data available => try send ready bytes
    auto res = send_ready_bytes();
    if (res < 0) {
        return -1;
    }

    return n;
}

int64_t send_stream::on_ack_recv(uint64_t acknum) {
    if (state_ == state::FINISHED) {
        // silently drop any acks after send stream finished
        return 0;
    }

    // check acknum in range
    if (acknum < una_) {
        // old ack - silently ignore
        return 0;
    }
    if (state_ == state::FIN_SENT && acknum > fin_ + 1) {
        // ack'd beyond fin - invalid
        Log(level::ERROR, std::format("acknum ({}) > fin ({}), i.e. peer ack'd beyond fin", acknum, fin_));
        return -1;
    }
    if ((state_ == state::ESTABLISHED || state_ == state::FIN_PENDING) && acknum > nxt_) {
        // ack'd beyond nxt - invalid
        Log(level::ERROR, std::format("acknum {} > nxt {}, i.e. acknum past any seqnums we've sent", acknum, nxt_));
        return -1;
    }

    // check for triple-dup-ack
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

    // advance una, popping segments of our in-flight-queue as necessary
    while (!in_flight_segments_.empty()) {
        segment &seg = in_flight_segments_.front();
        if (acknum < seg.seqnum) {
            // all now-acked in-flight-segments removed - stop
            break;
        }
        else if (seg.seqnum <= acknum && acknum < seg.seqnum + seg.payload_len) {
            // segment partially-acked - update
            uint64_t diff = acknum - seg.seqnum;
            seg.seqnum += diff;
            seg.payload_len -= diff;
            seg.payload_pos = inc(seg.payload_pos, diff);
            break;
        } 
        else {
            // fully acked segment - remove
            in_flight_segments_.pop_front();
        }
    }

    // advance una
    if (acknum == iss_ + 1 || acknum == fin_ + 1) {
        // syn or fin, don't advance una pos
    } else {
        una_pos_ = inc(una_pos_, acknum - una_);
    }
    una_ = acknum;
    

    // check if peer has ack'd our fin
    if (state_ == state::FIN_SENT) {
        if (acknum == fin_ + 1) {
            if (!in_flight_segments_.empty()) {
                Log(level::ERROR, "peer ack'd our fin, yet we still have in-flight bytes");
                return -1;
            }

            // peer validly ack'd fin - finish stream
            state_ = state::FINISHED;
        }
    }

    // clear current re-tx timer, start new one if unack'd bytes remain
    retransmission_timeout_->clear();
    if (!in_flight_segments_.empty()) {
        retransmission_timeout_->add();
    }

    // ack potentially made new data avail => try send ready bytes
    auto res = send_ready_bytes();
    if (res < 0) {
        return -1;
    }

    return 0;
}

int64_t send_stream::send_syn() {
    if (state_ != state::ESTABLISHED) {
        return -1;
    }

    uint16_t flags = syn_mask;
    auto res = send_segment_cb_(nxt_, flags, nullptr, 0);
    if (res < 0) {
        return -1;
    }

    if (buffer_segment_for_retransmission(segment{nxt_, flags, nxt_pos_, 0}) < 0) {
        return -1;
    }
    nxt_ += 1;

    return 0;
}

int64_t send_stream::send_syn_ack() {
    if (state_ != state::ESTABLISHED) {
        return -1;
    }

    uint16_t flags = syn_mask | ack_mask;
    auto res = send_segment_cb_(nxt_, flags, nullptr, 0);
    if (res < 0) {
        return -1;
    }

    if (buffer_segment_for_retransmission(segment{nxt_, flags, nxt_pos_, 0}) < 0) {
        return -1;
    }
    nxt_ += 1;

    return 0;
}

int64_t send_stream::send_fin() {
    if (state_ != state::ESTABLISHED) {
        return -1;
    }

    state_ = state::FIN_PENDING;
    return send_ready_bytes();
}

uint64_t send_stream::get_num_in_flight_bytes() {
    uint64_t capacity = buffer_->capacity();
    return ((nxt_pos_ + capacity) - una_pos_) % capacity;
}

uint64_t send_stream::get_num_ready_bytes() {
    uint64_t capacity = buffer_->capacity();
    return ((wr_pos_ + capacity) - nxt_pos_) % capacity;
}

uint64_t send_stream::get_num_free_space_bytes() {
    uint64_t capacity = buffer_->capacity();
    return ((una_pos_ + capacity) - (wr_pos_ + 1)) % capacity;
}

void send_stream::on_retransmission_timeout() {
    auto res = retransmit_oldest_segment();
    if (res < 0) {
        Log(level::ERROR, "retransmit_oldest_segment() failed");
        return;
    }

    retransmission_timeout_->restart();
    cong_->on_rto();
}

std::string send_stream::to_string(state s) {
    switch (s) {
        case state::ESTABLISHED: return "ESTABLISHED";
        case state::FIN_PENDING: return "FIN_PENDING";
        case state::FIN_SENT:    return "FIN_SENT";
        case state::FINISHED:    return "FINISHED";
    }
    return "UNKNOWN";
}

std::string send_stream::to_string() {
    std::ostringstream oss;
    oss << "send_stream" << "\n";
    oss << logging::divider << "\n";
    oss << "state: " << to_string(state_) << "\n";
    oss << "iss: " << iss_ << "\n";
    oss << "una: " << una_ << ", una_pos: " << una_pos_ << "\n";
    oss << "nxt: " << nxt_ << ", nxt_pos: " << nxt_pos_ << "\n";
    oss << "fin: " << fin_ << "\n";
    oss << "num in-flight segments: " << in_flight_segments_.size() << "\n";
    oss << "last acks: " << dup_ack_.last_acks[0] << " " << dup_ack_.last_acks[1] << "\n";
    oss << "rto: " << (retransmission_timeout_->active ? "active" : "inactive") << "\n";
    oss << "peer recv window: " << peer_recv_window_ << "\n";
    oss << "cong window: " << cong_->get_cwnd() << "\n";
    oss << logging::divider << "\n";
    return oss.str();
}

int64_t send_stream::send_ready_bytes() {
    while (get_num_ready_bytes() > 0 || state_ == state::FIN_PENDING) {
        uint64_t ready_bytes = get_num_ready_bytes();

        // truncate payload_len to min of: {ready_bytes, MSS, available_window}
        uint64_t payload_len = ready_bytes >= MSS ? MSS : ready_bytes;
        int64_t available_window = get_send_window() - get_num_in_flight_bytes();
        if (available_window <= 0) {
            // no available window, can't send now
            Log(level::INFO, "no available window to send");
            break;
        }
        if (payload_len > available_window) {
            payload_len = available_window;
        }

        uint16_t flags = ack_mask;

        // for last segment - tack on pending fin
        bool sending_fin = false;
        if (state_ == state::FIN_PENDING && payload_len == ready_bytes) {
            flags |= fin_mask;
            sending_fin = true;
        }

        // send segment
        uint8_t buf[payload_len];
        if (payload_len > 0) {
            buffer_->read(nxt_pos_, buf, payload_len);
        }
        auto res = send_segment_cb_(nxt_, flags, buf, payload_len);
        if (res < 0) {
            return -1;
        }

        // buffer for retransmission
        res = buffer_segment_for_retransmission(segment{nxt_, flags, nxt_pos_, payload_len});
        if (res < 0) {
            return -1;
        }

        // advance nxt
        nxt_ += payload_len;
        nxt_pos_ = inc(nxt_pos_, payload_len);
        if (sending_fin) {
            fin_ = nxt_;
            nxt_ += 1;
            state_ = state::FIN_SENT;
        }
    }

    return 0;
}

int64_t send_stream::buffer_segment_for_retransmission(const segment &seg) {
    if (seg.seqnum == una_) {
        if (!in_flight_segments_.empty()) {
            Log(level::ERROR, "sent segment with seqnum==una==nxt (implies no unack'd bytes exist), yet there are still unack'd bytes - invalid");
            return -1;
        }
        if (retransmission_timeout_->active) {
            Log(level::ERROR, "sent segment with seqnum==una==nxt (implies no unack'd bytes), yet rto timeout still active - invalid");
            return -1;
        }
        retransmission_timeout_->add();
    }
    in_flight_segments_.push_back(seg);

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
    uint64_t payload_len = seg.payload_len;

    // truncate payload_len to available window
    uint64_t available_window = get_send_window() - get_num_in_flight_bytes();
    available_window += payload_len; // this seg shouldn't contribute to the window
    if (available_window <= 0) {
        // no available window, can't send now (next re-tx will send, i.e. on rto or dup-ack)
        return 0;
    }
    if (payload_len > available_window) {
        payload_len = available_window;
    }

    // send segment
    uint8_t buf[payload_len];
    buffer_->read(seg.payload_pos, buf, payload_len);
    auto res = send_segment_cb_(seqnum, flags, buf, payload_len);
    if (res < 0) {
        return -1;
    }

    return 0;
}
