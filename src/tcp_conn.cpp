#include "tcp_conn.hpp"

tcp_conn::tcp_conn() {}

tcp_conn::~tcp_conn() {}

std::shared_ptr<tcp_conn> tcp_conn::open() {
    return std::shared_ptr<tcp_conn>(new tcp_conn(), [](tcp_conn *p) { delete p; });
}

int64_t tcp_conn::read(uint64_t n, uint8_t *dest_buffer) {
    return 0;
}

int64_t tcp_conn::write(uint64_t n, uint8_t *src_buffer) {
    return 0;
}

int64_t tcp_conn::close() {
    return 0;
}
