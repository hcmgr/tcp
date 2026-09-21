#include <gtest/gtest.h>
#include <cstring>
#include <string>

#include "../src/recv.hpp"

static const uint64_t TEST_CAPACITY = 64;

static std::string read_n(recv_stream &rs, uint64_t n) {
    std::string out(n, '\0');
    int64_t rc = rs.read(n, (uint8_t*)out.data());
    EXPECT_EQ(rc, (int64_t)n);
    return out;
}

TEST(recv_stream_test, contiguous_segments) {
    recv_stream rs(TEST_CAPACITY);
    uint64_t irs = 100;
    uint64_t seqnum;
    int64_t res;

    // syn
    res = rs.on_syn_recv(irs);
    EXPECT_EQ(res, 0);

    // recv segment
    seqnum = irs + 1;
    std::string seg1 = "hello";
    res = rs.recv_segment(seqnum, (uint8_t*)seg1.c_str(), seg1.size());
    EXPECT_EQ(res, 0);
    seqnum += seg1.size();

    EXPECT_EQ(rs.get_acknum(), seqnum);
    EXPECT_EQ(rs.get_num_ready_bytes(), seg1.size());
    EXPECT_FALSE(rs.is_finished());

    // recv another segment
    std::string seg2 = "world";
    res = rs.recv_segment(seqnum, (uint8_t*)seg2.c_str(), seg2.size());
    EXPECT_EQ(res, 0);
    seqnum += seg2.size();

    EXPECT_EQ(rs.get_acknum(), seqnum);
    EXPECT_EQ(rs.get_num_ready_bytes(), seg1.size() + seg2.size());
    EXPECT_LT(rs.get_num_free_space_bytes(), TEST_CAPACITY - 1);
    EXPECT_FALSE(rs.is_finished());

    // read all avail bytes
    std::string out = read_n(rs, seg1.size() + seg2.size());
    EXPECT_EQ(out, seg1 + seg2);

    EXPECT_EQ(rs.get_acknum(), seqnum); // hasn't moved as a result of read()
    EXPECT_EQ(rs.get_num_ready_bytes(), 0); // all bytes consumed
    EXPECT_EQ(rs.get_num_free_space_bytes(), TEST_CAPACITY - 1); // all spots free'd
    EXPECT_FALSE(rs.is_finished());

    // recv another segment
    std::string seg3 = "foobar";
    res = rs.recv_segment(seqnum, (uint8_t*)seg3.c_str(), seg3.size());
    EXPECT_EQ(res, 0);
    seqnum += seg3.size();

    EXPECT_EQ(rs.get_acknum(), seqnum);
    EXPECT_EQ(rs.get_num_ready_bytes(), seg3.size());

    // read some (not all) avail bytes
    uint64_t partial_n = 3;
    std::string part1 = read_n(rs, partial_n);
    EXPECT_EQ(part1, seg3.substr(0, partial_n));
    EXPECT_EQ(rs.get_acknum(), seqnum); // hasn't moved as a result of read()
    EXPECT_EQ(rs.get_num_ready_bytes(), seg3.size() - partial_n);

    // read rest avail bytes
    std::string part2 = read_n(rs, seg3.size() - partial_n);
    EXPECT_EQ(part2, seg3.substr(partial_n));
    EXPECT_EQ(rs.get_num_ready_bytes(), 0);
    EXPECT_EQ(rs.get_num_free_space_bytes(), TEST_CAPACITY - 1);

    // finish
    seqnum += 1; // fin consumes 1 seqnum
    res = rs.on_fin_recv(seqnum);
    EXPECT_TRUE(rs.is_finished());
}

TEST(recv_stream_test, out_of_order_segments) {
    recv_stream rs(TEST_CAPACITY);
    uint64_t irs = 100;
    int64_t res;

    res = rs.on_syn_recv(irs);
    EXPECT_EQ(res, 0);

    std::string seg1 = "hello";
    std::string seg2 = "world";
    uint64_t seg1_seq = irs + 1;
    uint64_t seg2_seq = seg1_seq + seg1.size();

    // recv second segment first - non-contiguous, should not advance nxt
    res = rs.recv_segment(seg2_seq, (uint8_t*)seg2.c_str(), seg2.size());
    EXPECT_EQ(res, 0);

    EXPECT_EQ(rs.get_acknum(), seg1_seq);
    EXPECT_EQ(rs.get_num_ready_bytes(), 0);

    // recv first segment - should now advance nxt past both segments
    res = rs.recv_segment(seg1_seq, (uint8_t*)seg1.c_str(), seg1.size());
    EXPECT_EQ(res, 0);

    EXPECT_EQ(rs.get_acknum(), seg2_seq + seg2.size());
    EXPECT_EQ(rs.get_num_ready_bytes(), seg1.size() + seg2.size());

    // verify combined bytes are readable and correctly ordered
    std::string out = read_n(rs, seg1.size() + seg2.size());
    EXPECT_EQ(out, seg1 + seg2);
}

TEST(recv_stream_test, capacity_and_wraparound) {
    recv_stream rs(TEST_CAPACITY);
    uint64_t irs = 100;
    int64_t res;

    res = rs.on_syn_recv(irs);
    EXPECT_EQ(res, 0);

    // fill recv buffer to capacity (TEST_CAPACITY - 1 usable bytes)
    uint64_t fill_size = TEST_CAPACITY - 1;
    std::string filler(fill_size, 'x');
    uint64_t seqnum = irs + 1;
    res = rs.recv_segment(seqnum, (uint8_t*)filler.c_str(), filler.size());
    EXPECT_EQ(res, 0);
    seqnum += filler.size();

    EXPECT_EQ(rs.get_num_free_space_bytes(), 0);

    // no free space left - further recv_segment should be rejected
    uint8_t extra = 'y';
    res = rs.recv_segment(seqnum, &extra, 1);
    EXPECT_EQ(res, -1);

    // drain buffer, confirming full capacity is freed again
    std::string drained = read_n(rs, fill_size);
    EXPECT_EQ(drained, filler);
    EXPECT_EQ(rs.get_num_free_space_bytes(), TEST_CAPACITY - 1);

    // many recv+read cycles so physical buffer positions wrap around multiple times
    for (int i = 0; i < 5; i++) {
        std::string chunk(20, (char)('a' + i));
        res = rs.recv_segment(seqnum, (uint8_t*)chunk.c_str(), chunk.size());
        EXPECT_EQ(res, 0);
        seqnum += chunk.size();

        std::string out = read_n(rs, chunk.size());
        EXPECT_EQ(out, chunk);
    }
}

TEST(recv_stream_test, abnormal_recvs) {
    recv_stream rs(TEST_CAPACITY);
    uint64_t irs = 100;
    int64_t res;

    res = rs.on_syn_recv(irs);
    EXPECT_EQ(res, 0);

    std::string seg1 = "hello";
    uint64_t seg1_seq = irs + 1;
    res = rs.recv_segment(seg1_seq, (uint8_t*)seg1.c_str(), seg1.size());
    EXPECT_EQ(res, 0);

    uint64_t acknum_after_seg1 = rs.get_acknum();
    uint64_t ready_after_seg1 = rs.get_num_ready_bytes();

    // segment already fully received - silent drop, no state change
    res = rs.recv_segment(seg1_seq, (uint8_t*)seg1.c_str(), seg1.size());
    EXPECT_EQ(res, 0);
    EXPECT_EQ(rs.get_acknum(), acknum_after_seg1);
    EXPECT_EQ(rs.get_num_ready_bytes(), ready_after_seg1);

    // segment already partially received - only new trailing bytes should be taken
    std::string overlap_seg = "lodog"; // last 2 bytes of "hello" ("lo") plus new "dog"
    uint64_t overlap_seq = seg1_seq + 3;
    res = rs.recv_segment(overlap_seq, (uint8_t*)overlap_seg.c_str(), overlap_seg.size());
    EXPECT_EQ(res, 0);

    EXPECT_EQ(rs.get_acknum(), acknum_after_seg1 + 3); // only "dog" is new
    EXPECT_EQ(rs.get_num_ready_bytes(), ready_after_seg1 + 3);

    std::string out = read_n(rs, ready_after_seg1 + 3);
    EXPECT_EQ(out, "hellodog");

    // on_fin_recv with bad fin value - postcondition fails, connection not finished
    uint64_t bad_fin = rs.get_acknum() + 100;
    res = rs.on_fin_recv(bad_fin);
    EXPECT_EQ(res, -1);
    EXPECT_FALSE(rs.is_finished());
}

TEST(recv_stream_test, ops_before_syn_and_ops_after_fin) {
    recv_stream rs(TEST_CAPACITY);
    int64_t res;
    uint8_t dest_buffer[16];

    // recv_segment/read before syn
    std::string early = "oops";
    res = rs.recv_segment(200, (uint8_t*)early.c_str(), early.size());
    EXPECT_EQ(res, -1); // not yet ESTABLISHED

    res = rs.read(early.size(), dest_buffer);
    EXPECT_EQ(res, -1); // not yet ESTABLISHED

    // establish, recv a segment, then finish
    uint64_t irs = 100;
    res = rs.on_syn_recv(irs);
    EXPECT_EQ(res, 0);

    std::string seg1 = "hello";
    uint64_t seqnum = irs + 1;
    res = rs.recv_segment(seqnum, (uint8_t*)seg1.c_str(), seg1.size());
    EXPECT_EQ(res, 0);
    seqnum += seg1.size();

    seqnum += 1; // fin consumes 1 seqnum
    res = rs.on_fin_recv(seqnum);
    EXPECT_EQ(res, 0);
    EXPECT_TRUE(rs.is_finished());

    // recv_segment after fin should fail
    res = rs.recv_segment(seqnum, (uint8_t*)early.c_str(), early.size());
    EXPECT_EQ(res, -1);

    // read after fin should fail
    res = rs.read(1, dest_buffer);
    EXPECT_EQ(res, -1);
}
