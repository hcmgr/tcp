#pragma once
#include <cstdint>
#include <memory>

class tcp_conn {
private:
    tcp_conn();
    ~tcp_conn();

public:
    static std::shared_ptr<tcp_conn> open();
    int64_t read(uint64_t n, uint8_t *dest_buffer);
    int64_t write(uint64_t n, uint8_t *src_buffer);
    int64_t close();
};
