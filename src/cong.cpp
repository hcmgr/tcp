#include "cong.hpp"

congestion_controller::congestion_controller()
    : cwnd_(INIT_CWND), ssthresh_(SSTHRESH) {}

congestion_controller::~congestion_controller() {}

//
// An ACK of new data arrived - grow cwnd.
//
int64_t congestion_controller::on_ack() {
    if (cwnd_ < ssthresh_) {
        //
        // slow start - exponential growth (cwnd roughly doubles each RTT).
        // there are ~cwnd/MSS acks per RTT, each adding MSS, so cwnd ~doubles over an RTT.
        //
        cwnd_ += MSS;
    }
    else {
        //
        // congestion avoidance - linear growth (cwnd grows ~1 MSS each RTT).
        // each of the ~cwnd/MSS acks per RTT adds MSS*MSS/cwnd, summing to ~1 MSS over the RTT.
        // the max(.., 1) keeps it from stalling once cwnd grows past MSS*MSS.
        //
        cwnd_ += std::max<int64_t>((int64_t)MSS * MSS / cwnd_, 1);
    }
    return cwnd_;
}

//
// Triple duplicate ACK (fast retransmit) - acks are still flowing, so we likely just dropped a
// single packet. Back off mildly: halve cwnd (down to ssthresh, floored at 2 MSS).
//
int64_t congestion_controller::on_triple_dup_ack() {
    ssthresh_ = std::max<int64_t>(cwnd_ / 2, 2 * MSS);
    cwnd_ = ssthresh_;
    return cwnd_;
}

//
// Retransmission timeout - no ack for RTO_TIMEOUT_MS, implying severe congestion. Back off hard:
// remember half the window as the new ssthresh, then drop cwnd to 1 MSS and restart slow start.
//
int64_t congestion_controller::on_rto() {
    ssthresh_ = std::max<int64_t>(cwnd_ / 2, 2 * MSS);
    cwnd_ = MSS;
    return cwnd_;
}

int64_t congestion_controller::get_cwnd() {
    return cwnd_;
}
