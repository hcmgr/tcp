#pragma once
#include <cstdint>
#include <memory>

#include "connection.hpp"

class tcp_conn {
private:
    std::shared_ptr<connection> conn_;

    tcp_conn(std::shared_ptr<connection> conn);
    ~tcp_conn();

public:
    static std::shared_ptr<tcp_conn> open(const conn_type &conn_type, const addr_tuple &addr_tuple);
    int64_t read(uint64_t n, uint8_t *dest_buffer);
    int64_t write(uint64_t n, uint8_t *src_buffer);
    int64_t close();
};
