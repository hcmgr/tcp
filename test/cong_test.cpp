#include <gtest/gtest.h>

#include "../src/cong.hpp"

TEST(cong_test, initial_state) {
    congestion_controller cc;
    EXPECT_EQ(cc.get_cwnd(), INIT_CWND);
}

TEST(cong_test, slow_start_growth) {
    congestion_controller cc;
    int64_t cwnd = cc.get_cwnd();

    while (cwnd < SSTHRESH) {
        int64_t next = cc.on_ack();
        EXPECT_EQ(next, cwnd + MSS);
        cwnd = next;
    }
}

TEST(cong_test, congestion_avoidance_growth) {
    congestion_controller cc;

    while (cc.get_cwnd() < SSTHRESH) {
        cc.on_ack();
    }

    int64_t cwnd = cc.get_cwnd();
    int64_t next = cc.on_ack();
    EXPECT_GT(next, cwnd);
    EXPECT_LT(next - cwnd, MSS);
}

TEST(cong_test, triple_dup_ack_halves_cwnd) {
    congestion_controller cc;

    for (int i = 0; i < 10; i++) {
        cc.on_ack();
    }

    int64_t cwnd = cc.get_cwnd();
    int64_t next = cc.on_triple_dup_ack();
    EXPECT_EQ(next, std::max<int64_t>(cwnd / 2, 2 * MSS));
    EXPECT_EQ(cc.get_cwnd(), next);
}

TEST(cong_test, triple_dup_ack_floor) {
    congestion_controller cc;
    cc.on_rto();

    int64_t next = cc.on_triple_dup_ack();
    EXPECT_EQ(next, 2 * MSS);
}

TEST(cong_test, rto_resets_cwnd) {
    congestion_controller cc;

    for (int i = 0; i < 10; i++) {
        cc.on_ack();
    }

    int64_t next = cc.on_rto();
    EXPECT_EQ(next, MSS);
    EXPECT_EQ(cc.get_cwnd(), MSS);
}

TEST(cong_test, rto_then_slow_start_uses_reduced_ssthresh) {
    congestion_controller cc;

    for (int i = 0; i < 10; i++) {
        cc.on_ack();
    }
    int64_t cwnd_before_rto = cc.get_cwnd();
    int64_t new_ssthresh = std::max<int64_t>(cwnd_before_rto / 2, 2 * MSS);
    cc.on_rto();

    while (cc.get_cwnd() < new_ssthresh) {
        int64_t before = cc.get_cwnd();
        int64_t next = cc.on_ack();
        EXPECT_EQ(next, before + MSS);
    }

    int64_t next = cc.on_ack();
    EXPECT_LT(next - new_ssthresh, MSS);
}
