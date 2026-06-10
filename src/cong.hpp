#include <cstdint>

#include "tcp_define.hpp"

//
// Classic-reno style congestion controller - call 'cong' for short.
//
// Cong maintains its internal cwnd, and updates cwnd on acks or congestion events:
//      onAck()
//      onTripleDupAck()
//      onRto()
// Each of these simply returns the updated cwnd.
//
// Classic-reno algorithm:
//      - start cwnd=INIT_CWND (e.g. 10 * MSS) 
//      - exponential growth
//          - cwnd doubles each RTT
//          - lasts until cwnd >= ssthresh, then enter linear growth
//      - linear growth
//          - cwnd increases by ~1MSS per RTT
//          - on triple dup ack (fast retransmit)
//              - cwnd = cwnd / 2 (halved)
//              - explanation
//                  - triple dup ack means packets are still flowing (i.e. still getting acks back),
//                    likely just missed a packet
//                  - so, halve it, not too extreme
//          - on rto
//              - cwnd = 1 MSS
//              - explanation
//                  - rto indicates haven't received ack for RTO_TIMER ms (e.g. 40 ms), which implies
//                    severe congestion (either your packets not getting through, or their acks, or both).
//                  - so, reduce back to 1 MSS, quite extreme
//      
//
class CongestionController {
private:
    int64_t cwnd;
    int64_t ssthresh;
public:
    CongestionController() 
        : cwnd(INIT_CWND), ssthresh(SSTHRESH) {}
    ~CongestionController() {}
public:
    int64_t onAck() {}
    int64_t onTripleDupAck() {}
    int64_t onRto() {}
public:
    int64_t getCwnd() { return cwnd; }
};