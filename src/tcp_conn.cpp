#include "tcp_conn.hpp"
#include "manager.hpp"

tcp_conn::tcp_conn(std::shared_ptr<connection> conn)
    : conn_(conn)
{}

tcp_conn::~tcp_conn() {}

std::shared_ptr<tcp_conn> tcp_conn::open(const conn_type &conn_type, const addr_tuple &addr_tuple) {
    auto conn = manager::get_instance().new_connection(addr_tuple);
    if (conn == nullptr) {
        return nullptr;
    }

    if (conn->open(conn_type) < 0) {
        Log(level::ERROR, "open() failed - connection handshake failed");
        return nullptr;
    }

    return std::shared_ptr<tcp_conn>(new tcp_conn(conn), [](tcp_conn *p) { delete p; });
}

int64_t tcp_conn::read(uint64_t n, uint8_t *dest_buffer) {
    return conn_->read(n, dest_buffer);
}

int64_t tcp_conn::write(uint64_t n, uint8_t *src_buffer) {
    return conn_->write(n, src_buffer);
}

int64_t tcp_conn::close() {
    return conn_->close();
}
