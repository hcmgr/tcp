#pragma once
#include <string>
#include <format>

#include "define.hpp"

//////////////////////////////////////////////////////////////////
// networking
//////////////////////////////////////////////////////////////////

namespace net {
    int create_udp_socket(const std::string& src_ip, const std::string& dst_ip, uint16_t src_port, uint16_t dst_port);

    bool tcp_checksum_valid(const tcp_header &hdr, uint8_t *payload, uint64_t payload_size);
    uint16_t tcp_checksum_calc(const tcp_header &hdr, uint8_t *payload, uint64_t payload_size);
}

//////////////////////////////////////////////////////////////////
// rng
//////////////////////////////////////////////////////////////////

namespace rng {
    uint32_t generate_iss();
}

//////////////////////////////////////////////////////////////////
// logging
//////////////////////////////////////////////////////////////////

#define Log(level, message) logging::log_impl((level), __FILE__, __LINE__, (message))

enum class level {
    ERROR,
    INFO
};

namespace logging {
    void log_impl(level level, const char *file, int line, const std::string &message);
};

