#include <algorithm>

#include "cong.hpp"

CongestionController::CongestionController()
    : cwnd(INIT_CWND), ssthresh(SSTHRESH) {}

CongestionController::~CongestionController() {}

//
// An ACK of new data arrived - grow cwnd.
//
int64_t CongestionController::onAck() {
    if (cwnd < ssthresh) {
        //
        // slow start - exponential growth (cwnd roughly doubles each RTT).
        // there are ~cwnd/MSS acks per RTT, each adding MSS, so cwnd ~doubles over an RTT.
        //
        cwnd += MSS;
    } else {
        //
        // congestion avoidance - linear growth (cwnd grows ~1 MSS each RTT).
        // each of the ~cwnd/MSS acks per RTT adds MSS*MSS/cwnd, summing to ~1 MSS over the RTT.
        // the max(.., 1) keeps it from stalling once cwnd grows past MSS*MSS.
        //
        cwnd += std::max<int64_t>((int64_t)MSS * MSS / cwnd, 1);
    }
    return cwnd;
}

//
// Triple duplicate ACK (fast retransmit) - acks are still flowing, so we likely just dropped a
// single packet. Back off mildly: halve cwnd (down to ssthresh, floored at 2 MSS).
//
int64_t CongestionController::onTripleDupAck() {
    ssthresh = std::max<int64_t>(cwnd / 2, 2 * MSS);
    cwnd = ssthresh;
    return cwnd;
}

//
// Retransmission timeout - no ack for RTO_TIMEOUT_MS, implying severe congestion. Back off hard:
// remember half the window as the new ssthresh, then drop cwnd to 1 MSS and restart slow start.
//
int64_t CongestionController::onRto() {
    ssthresh = std::max<int64_t>(cwnd / 2, 2 * MSS);
    cwnd = MSS;
    return cwnd;
}

int64_t CongestionController::getCwnd() {
    return cwnd;
}
