#include <chrono>
#include <cstring>
#include <ctime>
#include <iostream>
#include <random>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "utils.hpp"
#include "define.hpp"

//////////////////////////////////////////////////////////////////
// networking
//////////////////////////////////////////////////////////////////

namespace net {

    int create_udp_socket(const std::string& src_ip,
                          const std::string& dst_ip,
                          uint16_t src_port,
                          uint16_t dst_port) 
    {
        // create socket
        int fd = socket(AF_INET, SOCK_DGRAM, 17);
        if (fd == -1) {
            perror("socket");
            Log(level::ERROR, std::format("socket() - {}", strerror(errno)));
            return -1;
        }

        // bind local/source address
        sockaddr_in local_addr{};
        local_addr.sin_family = AF_INET;
        local_addr.sin_port = htons(src_port);
        if (inet_pton(AF_INET, src_ip.c_str(), &local_addr.sin_addr) != 1) {
            Log(level::ERROR, std::format("inet_pton() local_addr - {}", strerror(errno)));
            close(fd);
            return -1;
        }
        if (bind(fd, (sockaddr*)&local_addr, sizeof(local_addr)) == -1) {
            Log(level::ERROR, std::format("bind() local_addr - {}", strerror(errno)));
            close(fd);
            return -1;
        }

        // connect peer/dest address
        sockaddr_in peer_addr{};
        peer_addr.sin_family = AF_INET;
        peer_addr.sin_port = htons(dst_port);
        if (inet_pton(AF_INET, dst_ip.c_str(), &peer_addr.sin_addr) != 1) {
            Log(level::ERROR, std::format("inet_pton() peer_addr - {}", strerror(errno)));
            close(fd);
            return -1;
        }
        if (connect(fd, (sockaddr*)&peer_addr, sizeof(peer_addr)) == -1) {
            Log(level::ERROR, std::format("connect() peer_addr - {}", strerror(errno)));
            close(fd);
            return -1;
        }

        return fd;
    }

    namespace {
        uint32_t checksum_sum(const tcp_header &hdr, uint8_t *payload, uint64_t payload_size) {
            uint32_t sum = 0;

            const uint16_t *hdr_words = reinterpret_cast<const uint16_t*>(&hdr);
            for (size_t i = 0; i < sizeof(hdr) / 2; i++) {
                sum += hdr_words[i];
            }

            const uint16_t *payload_words = reinterpret_cast<const uint16_t*>(payload);
            uint64_t i = 0;
            for (; i + 1 < payload_size; i += 2) {
                sum += payload_words[i / 2];
            }
            if (i < payload_size) {
                sum += payload[i];
            }

            while (sum >> 16) {
                sum = (sum & 0xFFFF) + (sum >> 16);
            }

            return sum;
        }
    }

    uint16_t tcp_checksum_calc(const tcp_header &hdr, uint8_t *payload, uint64_t payload_size) {
        tcp_header tmp_hdr = hdr;
        tmp_hdr.checksum = 0;

        uint32_t sum = checksum_sum(tmp_hdr, payload, payload_size);
        return static_cast<uint16_t>(~sum);
    }

    bool tcp_checksum_valid(const tcp_header &hdr, uint8_t *payload, uint64_t payload_size) {
        uint32_t sum = checksum_sum(hdr, payload, payload_size);
        return static_cast<uint16_t>(~sum) == 0;
    }

}; // net

//////////////////////////////////////////////////////////////////
// rng
//////////////////////////////////////////////////////////////////

namespace rng {

    uint32_t generate_iss() {
        static thread_local std::mt19937 gen{std::random_device{}()};
        static thread_local std::uniform_int_distribution<uint32_t> dist(0, MAX_ISS - 1);
        return dist(gen);
    }

}; // rng

//////////////////////////////////////////////////////////////////
// logging
//////////////////////////////////////////////////////////////////

static level global_log_level = level::INFO;

namespace logging {

    namespace {
        bool check_log_level(level level) {
            return level <= global_log_level;
        }

        std::string level_name(level level) {
            switch (level) {
                case level::ERROR: return "ERROR";
                case level::INFO:  return "INFO";
            }
            return "UNKNOWN";
        }

        std::string utc_timestamp() {
            using namespace std::chrono;
            auto now = system_clock::now();
            auto secs = time_point_cast<seconds>(now);
            auto ms = duration_cast<milliseconds>(now - secs).count();

            std::time_t t = system_clock::to_time_t(now);
            std::tm tm{};
            gmtime_r(&t, &tm);

            char buf[32];
            std::size_t n = std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);

            char out[40];
            std::snprintf(out, sizeof(out), "%.*s.%03lldZ",
                        static_cast<int>(n), buf, static_cast<long long>(ms));
            return out;
        }

        std::string base_name(const char *path) {
            const char *slash = std::strrchr(path, '/');
            return slash ? slash + 1 : path;
        }
    }

    void log_impl(level level, const char *file, int line, const std::string &message) {
        if (!check_log_level(level)) {
            return;
        }
        std::ostream &os = (level == level::ERROR) ? std::cerr : std::cout;
        os << '[' << level_name(level) << ']'
        << '[' << utc_timestamp() << ']'
        << '[' << base_name(file) << ":" << line << ']' << " "
        << message << '\n';
    }
}; // logging