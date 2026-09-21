#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "../src/send.hpp"
#include "../src/manager.hpp"

struct sent_segment {
    uint64_t seqnum;
    uint16_t flags;
    std::vector<uint8_t> payload;
};
static std::vector<sent_segment> sent_segments;

int64_t mock_send_segment(uint64_t seqnum,
                          uint16_t flags,
                          uint8_t *payload_ptr,
                          uint64_t payload_len)
{
    sent_segment seg;
    seg.seqnum = seqnum;
    seg.flags = flags;
    if (payload_len > 0 && payload_ptr != nullptr) {
        seg.payload.assign(payload_ptr, payload_ptr + payload_len);
    }
    sent_segments.push_back(seg);
    return 0;
}

// test syn -> many segments sent/ack'd -> standalone fin
TEST(send_stream_test, send_many_segments) {
    sent_segments.clear();
    uint64_t capacity = 4096;
    send_stream ss(capacity, mock_send_segment);
    ss.set_peer_recv_window(UINT16_MAX);
    int64_t res;

    //
    // send syn
    //
    res = ss.send_syn();
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1u);
    EXPECT_LT(sent_segments[0].seqnum, (uint64_t)MAX_ISS);
    EXPECT_EQ(sent_segments[0].flags, syn_mask);
    EXPECT_TRUE(sent_segments[0].payload.empty());

    uint64_t seqnum = sent_segments[0].seqnum;
    uint64_t acknum = seqnum;
    seqnum += 1;
    sent_segments.clear();

    //
    // peer ack's our syn
    //
    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0u);
    EXPECT_TRUE(sent_segments.empty());


    //
    // write N bytes (N < get_num_free_space_bytes())
    //
    uint64_t n = 10;
    std::string data_n(n, 'x');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);

    EXPECT_EQ(ss.get_num_free_space_bytes(), capacity - 1 - n);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), n);
    EXPECT_EQ(ss.get_num_ready_bytes(), 0u);

    ASSERT_EQ(sent_segments.size(), 1u);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    EXPECT_EQ(sent_segments[0].flags, ack_mask);
    ASSERT_EQ(sent_segments[0].payload.size(), n);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data(), n), 0);

    seqnum += n;
    sent_segments.clear();

    //
    // peer acks some of the bytes
    //
    uint64_t partial = 4;
    acknum += partial;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), n - partial);
    EXPECT_EQ(ss.get_num_free_space_bytes(), capacity - 1 - (n - partial));
    EXPECT_TRUE(sent_segments.empty());

    //
    // peer acks the rest of the bytes
    //
    acknum += (n - partial);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0u);
    EXPECT_EQ(ss.get_num_free_space_bytes(), capacity - 1);
    EXPECT_TRUE(sent_segments.empty());

    //
    // write MSS + 1 bytes - only MSS bytes fit in a single segment
    //
    uint64_t big_n = MSS + 1;
    std::string data_big(big_n, 'y');
    res = ss.write(big_n, (uint8_t*)data_big.c_str());
    EXPECT_EQ(res, (int64_t)big_n);

    ASSERT_EQ(sent_segments.size(), 2u);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    EXPECT_EQ(sent_segments[0].flags, ack_mask);
    ASSERT_EQ(sent_segments[0].payload.size(), (uint64_t)MSS);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_big.data(), MSS), 0);

    seqnum += MSS;

    EXPECT_EQ(sent_segments[1].seqnum, seqnum);
    EXPECT_EQ(sent_segments[1].flags, ack_mask);
    ASSERT_EQ(sent_segments[1].payload.size(), 1u);
    EXPECT_EQ(sent_segments[1].payload[0], (uint8_t)'y');

    EXPECT_EQ(ss.get_num_in_flight_bytes(), big_n);
    EXPECT_EQ(ss.get_num_ready_bytes(), 0u);

    seqnum += 1;
    sent_segments.clear();

    //
    // peer acks those bytes
    //
    acknum += big_n;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0u);
    EXPECT_EQ(ss.get_num_free_space_bytes(), capacity - 1);
    EXPECT_TRUE(sent_segments.empty());

    //
    // write M bytes
    //
    uint64_t m = 50;
    std::string data_m(m, 'z');
    res = ss.write(m, (uint8_t*)data_m.c_str());
    EXPECT_EQ(res, (int64_t)m);

    ASSERT_EQ(sent_segments.size(), 1u);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    EXPECT_EQ(sent_segments[0].flags, ack_mask);
    ASSERT_EQ(sent_segments[0].payload.size(), m);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_m.data(), m), 0);

    seqnum += m;
    sent_segments.clear();

    //
    // peer acks those bytes
    //
    acknum += m;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0u);
    EXPECT_TRUE(sent_segments.empty());

    //
    // send fin - standalone, no attached bytes
    //
    res = ss.send_fin();
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1u);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    EXPECT_EQ(sent_segments[0].flags, (uint16_t)(ack_mask | fin_mask));
    EXPECT_TRUE(sent_segments[0].payload.empty());

    EXPECT_FALSE(ss.is_finished());

    sent_segments.clear();

    //
    // peer acks the fin - stream should be finished
    //
    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_TRUE(ss.is_finished());
}

// test fin tack'd onto last segment (rather than stand-alone segment)
TEST(send_stream_test, fin_with_last_segment) {
    sent_segments.clear();
    uint64_t capacity = 4096;
    send_stream ss(capacity, mock_send_segment);
    ss.set_peer_recv_window(UINT16_MAX);
    int64_t res;

    //
    // send syn, peer ack's fin (successful handshake)
    //
    res = ss.send_syn();
    EXPECT_EQ(res, 0);

    uint64_t seqnum = sent_segments[0].seqnum;
    uint64_t acknum = seqnum;
    seqnum += 1;
    sent_segments.clear();

    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    //
    // a) set recv window to 0 to stop sending
    // b) send segment with N > 0, so the bytes must buffer
    // c) send a fin, which must then 'queue' behind those waiting bytes
    //
    ss.set_peer_recv_window(0);
    uint64_t n = 10;
    std::string data_n(n, 'n');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);
    EXPECT_EQ(sent_segments.size(), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)0);

    res = ss.send_fin();
    EXPECT_EQ(res, 0);
    EXPECT_EQ(sent_segments.size(), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)0);

    //
    // reset recv window to large value, send ack to trigger a send of ready bytes + our fin
    //
    ss.set_peer_recv_window(UINT16_MAX);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)10);

    //
    // ack the n segment bytes + the fin
    //
    acknum += (n + 1);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)0);
    EXPECT_TRUE(ss.is_finished());
}

// test trip dup ack
TEST(send_stream_test, trip_dup_ack) {
    sent_segments.clear();
    uint64_t capacity = 4096;
    send_stream ss(capacity, mock_send_segment);
    ss.set_peer_recv_window(UINT16_MAX);
    int64_t res;

    //
    // send syn, peer ack's fin (successful handshake)
    //
    res = ss.send_syn();
    EXPECT_EQ(res, 0);

    uint64_t seqnum = sent_segments[0].seqnum;
    uint64_t acknum = seqnum;
    seqnum += 1;
    sent_segments.clear();

    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    //
    // send segment with N > 0 payload, this segment gets dropped
    //
    uint64_t n = 10;
    std::string data_n(n, 'n');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);
    EXPECT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)n);

    //
    // first dup ack - expect no re-tx
    //
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)n);

    //
    // send another segment with M > 0 payload, this segment arrives
    //
    uint64_t m = 15;
    std::string data_m(m, 'm');
    res = ss.write(m, (uint8_t*)data_m.c_str());
    EXPECT_EQ(res, (int64_t)m);
    EXPECT_EQ(sent_segments.size(), 2);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)(n + m));

    //
    // second dup ack - expect no re-tx
    //
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(sent_segments.size(), 2);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)(n + m));

    //
    // send another segment with K > 0 payload, this segment arrives
    //
    uint64_t k = 20;
    std::string data_k(k, 'k');
    res = ss.write(k, (uint8_t*)data_k.c_str());
    EXPECT_EQ(res, (int64_t)k);
    EXPECT_EQ(sent_segments.size(), 3);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)(n + m + k));

    //
    // third dup ack - expect re-tx of oldest segment
    //
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(sent_segments.size(), 4);                             
    EXPECT_EQ(ss.get_num_in_flight_bytes(), (int64_t)(n + m + k));

    //
    // ack all 3 segments, mocking that: re-tx of first segment worked, so now all 3 segments
    // can be cumulatively acked
    //
    acknum += (n + m + k);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0);
}

// test rto triggers re-tx of oldest un-ack'd segment
TEST(send_stream_test, rto) {
    sent_segments.clear();
    uint64_t capacity = 4096;
    send_stream ss(capacity, mock_send_segment);
    ss.set_peer_recv_window(UINT16_MAX);
    int64_t res;

    //
    // send syn, peer ack's syn (successful handshake)
    //
    res = ss.send_syn();
    EXPECT_EQ(res, 0);

    uint64_t seqnum = sent_segments[0].seqnum;
    uint64_t acknum = seqnum;
    seqnum += 1;
    sent_segments.clear();

    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    //
    // send 2 segments, N and M bytes, both get dropped, nothing sends until rto
    //
    uint64_t n = 10;
    std::string data_n(n, 'n');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);

    uint64_t m = 15;
    std::string data_m(m, 'm');
    res = ss.write(m, (uint8_t*)data_m.c_str());
    EXPECT_EQ(res, (int64_t)m);

    EXPECT_EQ(sent_segments.size(), 2);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), n + m);

    //
    // rto fires - expect re-tx of first segment only
    //
    ss.on_retransmission_timeout();

    EXPECT_EQ(sent_segments.size(), 3);
    EXPECT_EQ(sent_segments[2].seqnum, seqnum);
    EXPECT_EQ(sent_segments[2].flags, ack_mask);
    ASSERT_EQ(sent_segments[2].payload.size(), n);
    EXPECT_EQ(std::memcmp(sent_segments[2].payload.data(), data_n.data(), n), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), n + m);

    seqnum += n;

    //
    // peer acks the first segment
    //
    acknum += n;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), m);

    //
    // rto fires again - expect re-tx of second segment
    //
    ss.on_retransmission_timeout();

    EXPECT_EQ(sent_segments.size(), 4);
    EXPECT_EQ(sent_segments[3].seqnum, seqnum);
    EXPECT_EQ(sent_segments[3].flags, ack_mask);
    ASSERT_EQ(sent_segments[3].payload.size(), m);
    EXPECT_EQ(std::memcmp(sent_segments[3].payload.data(), data_m.data(), m), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), m);

    //
    // peer acks the second segment
    //
    acknum += m;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0);
}

// test sends being truncated by peer's recv window
TEST(send_stream_test, send_limited_by_recv_window) {
    sent_segments.clear();
    uint64_t capacity = 4096;
    send_stream ss(capacity, mock_send_segment);
    ss.set_peer_recv_window(UINT16_MAX);
    int64_t res;

    //
    // send syn, peer ack's syn (successful handshake)
    //
    res = ss.send_syn();
    EXPECT_EQ(res, 0);

    uint64_t seqnum = sent_segments[0].seqnum;
    uint64_t acknum = seqnum;
    seqnum += 1;
    sent_segments.clear();

    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    //
    // recv window M, write N > M, expect only M sent
    //
    uint64_t m = 100;
    ss.set_peer_recv_window(m);

    uint64_t n = 250;
    std::string data_n(n, 'n');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    EXPECT_EQ(sent_segments[0].flags, ack_mask);
    ASSERT_EQ(sent_segments[0].payload.size(), m);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data(), m), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), m);
    EXPECT_EQ(ss.get_num_ready_bytes(), n - m);

    seqnum += m;
    sent_segments.clear();

    //
    // peer acks those M bytes - expect next M sent
    //
    acknum += m;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    ASSERT_EQ(sent_segments[0].payload.size(), m);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data() + m, m), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), m);
    EXPECT_EQ(ss.get_num_ready_bytes(), n - 2 * m);

    seqnum += m;
    sent_segments.clear();

    //
    // peer acks those M bytes - expect remaining N - 2M sent
    //
    acknum += m;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    ASSERT_EQ(sent_segments[0].payload.size(), n - 2 * m);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data() + 2 * m, n - 2 * m), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), n - 2 * m);
    EXPECT_EQ(ss.get_num_ready_bytes(), 0);

    seqnum += (n - 2 * m);
    sent_segments.clear();

    acknum += (n - 2 * m);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0);
    EXPECT_TRUE(sent_segments.empty());

    //
    // recv window M, K < M bytes in-flight, write N > (M - K), expect only (M - K) sent (total M bytes in-flight)
    //
    uint64_t k = 40;
    std::string data_k(k, 'k');
    res = ss.write(k, (uint8_t*)data_k.c_str());
    EXPECT_EQ(res, (int64_t)k);
    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), k);

    seqnum += k;
    sent_segments.clear();

    n = 150;
    data_n = std::string(n, 'N');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    ASSERT_EQ(sent_segments[0].payload.size(), m - k);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data(), m - k), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), m);
    EXPECT_EQ(ss.get_num_ready_bytes(), n - (m - k));

    seqnum += (m - k);
    sent_segments.clear();

    //
    // re-open recv window, peer acks the K bytes - expect rest of N sent
    //
    ss.set_peer_recv_window(UINT16_MAX);
    acknum += k;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    ASSERT_EQ(sent_segments[0].payload.size(), n - (m - k));
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data() + (m - k), n - (m - k)), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), n);
    EXPECT_EQ(ss.get_num_ready_bytes(), 0);

    sent_segments.clear();

    acknum += n;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0);
    EXPECT_TRUE(sent_segments.empty());
}

struct mock_congestion_controller : public congestion_controller {
    int64_t cwnd;

    mock_congestion_controller(int64_t cwnd) : cwnd(cwnd) {}

    int64_t on_ack() override { return cwnd; }
    int64_t on_triple_dup_ack() override { return cwnd; }
    int64_t on_rto() override { return cwnd; }
    int64_t get_cwnd() override { return cwnd; }
};

// test sends being truncated by congestion window (cong mocked, see cong_test.cpp for UT of our real cong)
TEST(send_stream_test, send_limited_by_cong_window) {
    sent_segments.clear();
    uint64_t capacity = 4096 * 4;
    auto cong_ptr = std::make_unique<mock_congestion_controller>(3 * MSS);
    mock_congestion_controller *cong = cong_ptr.get();
    send_stream ss(capacity, mock_send_segment, std::move(cong_ptr));
    ss.set_peer_recv_window(UINT16_MAX);
    int64_t res;

    //
    // send syn, peer ack's syn (successful handshake)
    //
    res = ss.send_syn();
    EXPECT_EQ(res, 0);

    uint64_t seqnum = sent_segments[0].seqnum;
    uint64_t acknum = seqnum;
    seqnum += 1;
    sent_segments.clear();

    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    //
    // cwnd = 3 * MSS, write 5 * MSS bytes - expect only 3 segments sent
    //
    uint64_t n = 5 * MSS;
    std::string data_n(n, 'c');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);

    ASSERT_EQ(sent_segments.size(), 3);
    for (uint64_t i = 0; i < 3; i++) {
        EXPECT_EQ(sent_segments[i].seqnum, seqnum + i * MSS);
        EXPECT_EQ(sent_segments[i].flags, ack_mask);
        ASSERT_EQ(sent_segments[i].payload.size(), (uint64_t)MSS);
        EXPECT_EQ(std::memcmp(sent_segments[i].payload.data(), data_n.data() + i * MSS, MSS), 0);
    }
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 3 * MSS);
    EXPECT_EQ(ss.get_num_ready_bytes(), 2 * MSS);

    seqnum += 3 * MSS;
    sent_segments.clear();

    //
    // peer acks one segment, cwnd unchanged - expect 1 more segment sent
    //
    acknum += MSS;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    ASSERT_EQ(sent_segments[0].payload.size(), (uint64_t)MSS);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data() + 3 * MSS, MSS), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 3 * MSS);
    EXPECT_EQ(ss.get_num_ready_bytes(), (uint64_t)MSS);

    seqnum += MSS;
    sent_segments.clear();

    //
    // grow cwnd, peer acks one segment - expect remaining segment sent
    //
    cong->cwnd = 10 * MSS;
    acknum += MSS;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    ASSERT_EQ(sent_segments[0].payload.size(), (uint64_t)MSS);
    EXPECT_EQ(std::memcmp(sent_segments[0].payload.data(), data_n.data() + 4 * MSS, MSS), 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 3 * MSS);
    EXPECT_EQ(ss.get_num_ready_bytes(), 0);

    sent_segments.clear();

    //
    // peer acks all remaining bytes
    //
    acknum += 3 * MSS;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0);
    EXPECT_TRUE(sent_segments.empty());
}

// test public api calls in invalid states
TEST(send_stream_test, ops_in_bad_states) {
    sent_segments.clear();
    uint64_t capacity = 4096;
    int64_t res;

    //
    // fin before syn
    //
    {
        send_stream ss(capacity, mock_send_segment);
        ss.set_peer_recv_window(UINT16_MAX);

        res = ss.send_fin();
        EXPECT_EQ(res, -1);
        EXPECT_TRUE(sent_segments.empty());
    }
    sent_segments.clear();

    send_stream ss(capacity, mock_send_segment);
    ss.set_peer_recv_window(UINT16_MAX);

    //
    // send syn, peer ack's syn (successful handshake)
    //
    res = ss.send_syn();
    EXPECT_EQ(res, 0);

    uint64_t seqnum = sent_segments[0].seqnum;
    uint64_t acknum = seqnum;
    seqnum += 1;
    sent_segments.clear();

    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    //
    // close recv window, write N bytes + send fin ==> both stay queued
    //
    ss.set_peer_recv_window(0);
    uint64_t n = 10;
    std::string data_n(n, 'n');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);

    res = ss.send_fin();
    EXPECT_EQ(res, 0);
    EXPECT_TRUE(sent_segments.empty());

    //
    // fin pending - write/fin/syn all rejected, ack beyond nxt rejected
    //
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, -1);

    res = ss.send_fin();
    EXPECT_EQ(res, -1);

    res = ss.send_syn();
    EXPECT_EQ(res, -1);

    res = ss.on_ack_recv(acknum + 1);
    EXPECT_EQ(res, -1);

    EXPECT_TRUE(sent_segments.empty());
    EXPECT_EQ(ss.get_num_ready_bytes(), n);

    //
    // re-open recv window, ack triggers send of N bytes + fin (fin sent)
    //
    ss.set_peer_recv_window(UINT16_MAX);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    ASSERT_EQ(sent_segments.size(), 1);
    EXPECT_EQ(sent_segments[0].seqnum, seqnum);
    EXPECT_EQ(sent_segments[0].flags, (uint16_t)(ack_mask | fin_mask));
    ASSERT_EQ(sent_segments[0].payload.size(), n);
    sent_segments.clear();

    //
    // fin sent - write/fin/syn rejected, ack beyond fin rejected
    //
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, -1);

    res = ss.send_fin();
    EXPECT_EQ(res, -1);

    res = ss.send_syn();
    EXPECT_EQ(res, -1);

    res = ss.on_ack_recv(acknum + n + 2);
    EXPECT_EQ(res, -1);

    EXPECT_TRUE(sent_segments.empty());
    EXPECT_FALSE(ss.is_finished());

    //
    // peer acks N bytes + fin - stream finished
    //
    acknum += (n + 1);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_TRUE(ss.is_finished());
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0);

    //
    // finished - write/fin/syn rejected, acks silently dropped
    //
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, -1);

    res = ss.send_fin();
    EXPECT_EQ(res, -1);

    res = ss.send_syn();
    EXPECT_EQ(res, -1);

    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);

    res = ss.on_ack_recv(acknum + 100);
    EXPECT_EQ(res, 0);

    EXPECT_TRUE(sent_segments.empty());
    EXPECT_TRUE(ss.is_finished());
}