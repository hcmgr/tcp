#include <gtest/gtest.h>
#include <cstring>
#include <string>

#include "../src/recv.hpp"

namespace {
constexpr uint64_t TEST_CAPACITY = 64;

std::string read_n(recv_stream &rs, uint64_t n) {
    std::string out(n, '\0');
    int64_t rc = rs.read(n, (uint8_t*)out.data());
    EXPECT_EQ(rc, (int64_t)n);
    return out;
}
}

//
// on_syn() -> contiguous recv_segment() calls -> on_fin()
//
TEST(RecvStreamTest, SynContiguousSegmentsThenFin) {
    recv_stream rs(TEST_CAPACITY);

    uint64_t irs = 100;
    rs.on_syn(irs);

    std::string a = "hello";
    std::string b = "world";
    std::string c = "!";

    uint64_t seq_a = irs + 1;
    uint64_t seq_b = seq_a + a.size();
    uint64_t seq_c = seq_b + b.size();

    EXPECT_EQ(rs.recv_segment(seq_a, (uint8_t*)a.data(), a.size()), (int64_t)a.size());
    EXPECT_EQ(rs.ready_bytes(), (int64_t)a.size());

    EXPECT_EQ(rs.recv_segment(seq_b, (uint8_t*)b.data(), b.size()), (int64_t)b.size());
    EXPECT_EQ(rs.recv_segment(seq_c, (uint8_t*)c.data(), c.size()), (int64_t)c.size());

    EXPECT_EQ(rs.ready_bytes(), (int64_t)(a.size() + b.size() + c.size()));
    EXPECT_EQ(read_n(rs, a.size() + b.size() + c.size()), a + b + c);

    uint64_t fin = seq_c + c.size();
    rs.on_fin(fin);

    // stream is torn down - further segments are dropped
    std::string late = "late";
    EXPECT_EQ(rs.recv_segment(fin, (uint8_t*)late.data(), late.size()), -1);
}

//
// on_syn() -> out-of-order recv_segment() calls -> on_fin()
//
TEST(RecvStreamTest, SynOutOfOrderSegmentsThenFin) {
    recv_stream rs(TEST_CAPACITY);

    uint64_t irs = 0;
    rs.on_syn(irs);

    std::string a = "AAAA";
    std::string b = "BBBB";
    std::string c = "CCCC";

    uint64_t seq_a = irs + 1;
    uint64_t seq_b = seq_a + a.size();
    uint64_t seq_c = seq_b + b.size();

    // deliver out-of-order: C, then A, then B
    EXPECT_EQ(rs.recv_segment(seq_c, (uint8_t*)c.data(), c.size()), (int64_t)c.size());
    // nothing contiguous with nxt_ yet - not ready to read
    EXPECT_EQ(rs.ready_bytes(), 0);

    EXPECT_EQ(rs.recv_segment(seq_a, (uint8_t*)a.data(), a.size()), (int64_t)a.size());
    // only A is contiguous with nxt_ - B/C still pending
    EXPECT_EQ(rs.ready_bytes(), (int64_t)a.size());

    EXPECT_EQ(rs.recv_segment(seq_b, (uint8_t*)b.data(), b.size()), (int64_t)b.size());
    // B arriving closes the gap, so B and the earlier C both become readable
    EXPECT_EQ(rs.ready_bytes(), (int64_t)(a.size() + b.size() + c.size()));

    EXPECT_EQ(read_n(rs, a.size() + b.size() + c.size()), a + b + c);

    uint64_t fin = seq_c + c.size();
    rs.on_fin(fin);
    std::string late = "x";
    EXPECT_EQ(rs.recv_segment(fin, (uint8_t*)late.data(), late.size()), -1);
}

//
// duplicate / overlapping segments are trimmed or dropped rather than corrupting the stream
//
TEST(RecvStreamTest, DuplicateAndOverlappingSegmentsAreTrimmed) {
    recv_stream rs(TEST_CAPACITY);

    uint64_t irs = 0;
    rs.on_syn(irs);

    std::string a = "0123456789";
    uint64_t seq_a = irs + 1;
    EXPECT_EQ(rs.recv_segment(seq_a, (uint8_t*)a.data(), a.size()), (int64_t)a.size());

    // fully-duplicate retransmit of A - silently dropped, no change
    EXPECT_EQ(rs.recv_segment(seq_a, (uint8_t*)a.data(), a.size()), 0);
    EXPECT_EQ(rs.ready_bytes(), (int64_t)a.size());

    // partial overlap with already-received bytes - only the new tail is taken
    std::string overlap_tail = "6789XY";
    uint64_t seq_overlap = seq_a + 6; // bytes irs+7..irs+10 already received
    EXPECT_EQ(rs.recv_segment(seq_overlap, (uint8_t*)overlap_tail.data(), overlap_tail.size()), 2); // only "XY" is new
    EXPECT_EQ(rs.ready_bytes(), (int64_t)(a.size() + 2));
    EXPECT_EQ(read_n(rs, a.size() + 2), a + "XY");
}

//
// ring-buffer wraparound: repeated write/read cycles must exceed capacity without corrupting data
//
TEST(RecvStreamTest, BufferWrapsAroundAcrossMultipleCycles) {
    recv_stream rs(TEST_CAPACITY);

    uint64_t irs = 0;
    rs.on_syn(irs);

    uint64_t seq = irs + 1;
    for (int i = 0; i < 10; i++) {
        std::string chunk = std::to_string(i) + "-chunk"; // ~8 bytes, forces several wraps over TEST_CAPACITY=64

        ASSERT_EQ(rs.recv_segment(seq, (uint8_t*)chunk.data(), chunk.size()), (int64_t)chunk.size());
        ASSERT_EQ(rs.ready_bytes(), (int64_t)chunk.size());
        EXPECT_EQ(read_n(rs, chunk.size()), chunk);

        seq += chunk.size();
    }

    EXPECT_EQ(rs.ready_bytes(), 0);
}

//
// state guards: recv_segment()/on_fin() outside of ESTABLISHED are safely dropped
//
TEST(RecvStreamTest, StateGuardsRejectCallsOutsideEstablished) {
    recv_stream rs(TEST_CAPACITY);

    // recv_segment() before on_syn() - still SYN_WAITING
    std::string early = "nope";
    EXPECT_EQ(rs.recv_segment(0, (uint8_t*)early.data(), early.size()), -1);

    // on_fin() before on_syn() - dropped, does not disturb SYN_WAITING
    rs.on_fin(123);

    // on_syn() should still succeed since the bogus on_fin() above had no effect
    uint64_t irs = 5;
    rs.on_syn(irs);

    std::string data = "ok";
    uint64_t seq = irs + 1;
    EXPECT_EQ(rs.recv_segment(seq, (uint8_t*)data.data(), data.size()), (int64_t)data.size());
    EXPECT_EQ(read_n(rs, data.size()), data);

    uint64_t fin = seq + data.size();
    rs.on_fin(fin);

    // recv_segment() after on_fin() - FINISHED state drops further segments
    std::string late = "late";
    EXPECT_EQ(rs.recv_segment(fin, (uint8_t*)late.data(), late.size()), -1);
}
