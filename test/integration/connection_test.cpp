#include <gtest/gtest.h>

#include "setup.hpp"
#include "../../src/connection.hpp"
#include "../../src/manager.hpp"

constexpr uint16_t CONN_PORT = 10000;
constexpr uint16_t MOCK_PEER_PORT = 10001;

TEST(handshake, connect) {
    mock_peer mock_peer_(MOCK_PEER_PORT, CONN_PORT);
    connection_thread conn_thread;

    auto conn = conn_thread.post([] {
        return manager::get_instance().create_connection({"127.0.0.1", "127.0.0.1", CONN_PORT, MOCK_PEER_PORT});
    }).get();
    ASSERT_NE(conn, nullptr);

    auto opened = conn_thread.post([conn] { return conn->open(conn_type::CONNECT); });

    auto syn = mock_peer_.recv();
    ASSERT_TRUE(syn.has_value()) << "no SYN received";
    EXPECT_EQ(syn->hdr.flags, syn_mask);
    EXPECT_EQ(syn->hdr.src_port, CONN_PORT);
    EXPECT_EQ(syn->hdr.dest_port, MOCK_PEER_PORT);
    EXPECT_TRUE(net::tcp_checksum_valid(syn->hdr, syn->payload.data(), syn->payload.size()));

    uint32_t peer_iss = 5000;
    mock_peer_.send(peer_iss, syn->hdr.seqnum + 1, syn_mask | ack_mask);

    auto ack = mock_peer_.recv();
    ASSERT_TRUE(ack.has_value()) << "no ACK received";
    EXPECT_EQ(ack->hdr.flags, ack_mask);
    EXPECT_EQ(ack->hdr.seqnum, syn->hdr.seqnum + 1);
    EXPECT_EQ(ack->hdr.acknum, peer_iss + 1);

    EXPECT_EQ(opened.get(), 0);
    EXPECT_EQ(conn_thread.post([conn] { return conn->get_state(); }).get(), tcp_state::ESTABLISHED);
}
