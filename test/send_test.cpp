/*

mocking
    - to capture the packets send_stream sends out, we'll pass it
      our own send_segment_cb_, which can capture the values it passed it,
      and do some verification on it
    - we use this to verify syn/fin sends, normal segment sends, and 
      re-tx segment sends

cases
    - normal send
        - send_syn() for initial syn
        - on_ack_recv() for peer's ack of syn
        - write() N bytes (N < get_num_free_space_bytes())
            - check nums changed accordingly
            - check cb called correctly
        - on_ack_recv() some of the bytes
            - check nums changed
        - on_ack_recv() rest of bytes
            - check nums changed
        - write() MSS + 1 bytes
            - check only MSS sent
        - on_ack_recv() those bytes
        - write() M bytes
            - check M + 1 are sent
        - on_ack_recv() those bytes
        - send_fin()
            - normal, standalone fin, no attached bytes
        - check finished

    - fin w/ last segment
        - i.e. exercise scenario where a send_fin() call has been
        made whilst there are still unsent bytes (for some reason)
            - verify send_segment_cb called with correct vals

    - trip dup ack

    - rto triggered

    - exercise being limited by peer recv window
        - case 1
            - set_peer_recv_window = M
            - [have no ready / in-flight bytes]
            - write() N > M
            - verify N sent
        - case 2
    
    - exercise being limited by congestion window
        - use the define.hpp to guide how far you have
          to push things to exercise it
        - keep in mind, we have separate cong_test.cpp UT now,
          so this ought not be a complete UT 

    - calls in bad states
        - fin before syn
        - syn after fin
        - write() / on_ack_recv() (main public APIs) after fin
        - fin_pending / fin_sent state stuff

*/

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "../src/send.hpp"
#include "../src/manager.hpp"

static const uint64_t TEST_CAPACITY = 4096;


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

TEST(send_stream_test, normal_send) {
    sent_segments.clear();
    send_stream ss(TEST_CAPACITY, mock_send_segment);
    int64_t res;

    EXPECT_EQ(ss.get_num_free_space_bytes(), TEST_CAPACITY - 1);

    ss.set_peer_recv_window(UINT16_MAX);

    std::cout << "1: " << ss.to_string() << "\n";

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

    std::cout << "2: " << ss.to_string() << "\n";

    //
    // peer ack's our syn
    //
    acknum += 1;
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0u);
    EXPECT_TRUE(sent_segments.empty());

    std::cout << "4: " << ss.to_string() << "\n";

    //
    // write N bytes (N < get_num_free_space_bytes())
    //
    uint64_t n = 10;
    std::string data_n(n, 'x');
    res = ss.write(n, (uint8_t*)data_n.c_str());
    EXPECT_EQ(res, (int64_t)n);

    std::cout << "5: " << ss.to_string() << "\n";

    EXPECT_EQ(ss.get_num_free_space_bytes(), TEST_CAPACITY - 1 - n);
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
    EXPECT_EQ(ss.get_num_free_space_bytes(), TEST_CAPACITY - 1 - (n - partial));
    EXPECT_TRUE(sent_segments.empty());

    std::cout << "7: " << ss.to_string() << "\n";

    //
    // peer acks the rest of the bytes
    //
    acknum += (n - partial);
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0u);
    EXPECT_EQ(ss.get_num_free_space_bytes(), TEST_CAPACITY - 1);
    EXPECT_TRUE(sent_segments.empty());

    std::cout << "8: " << ss.to_string() << "\n";

    //
    // write MSS + 1 bytes - only MSS bytes fit in a single segment
    //
    uint64_t big_n = MSS + 1;
    std::string data_big(big_n, 'y');
    res = ss.write(big_n, (uint8_t*)data_big.c_str());
    EXPECT_EQ(res, (int64_t)big_n);

    std::cout << "9: " << ss.to_string() << "\n";

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
    EXPECT_EQ(ss.get_num_free_space_bytes(), TEST_CAPACITY - 1);
    EXPECT_TRUE(sent_segments.empty());

    std::cout << "10: " << ss.to_string() << "\n";

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

    std::cout << "11: " << ss.to_string() << "\n";

    //
    // peer acks those bytes
    //
    acknum += m;
    std::cout << "sending acknum - " << acknum << "\n";
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_EQ(ss.get_num_in_flight_bytes(), 0u);
    EXPECT_TRUE(sent_segments.empty());

    std::cout << "12: " << ss.to_string() << "\n";

    //
    // send fin - standalone, no attached bytes
    //
    res = ss.send_fin();
    EXPECT_EQ(res, 0);

    std::cout << "13: " << ss.to_string() << "\n";

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
    std::cout << "sending acknum - " << acknum << "\n";
    res = ss.on_ack_recv(acknum);
    EXPECT_EQ(res, 0);
    EXPECT_TRUE(ss.is_finished());
    std::cout << "14: " << ss.to_string() << "\n";
}

TEST(send_stream_test, trip_dup_ack) {

}
