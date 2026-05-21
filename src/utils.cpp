#include "utils.hpp"

#include <chrono>
#include <cstring>
#include <ctime>
#include <iostream>

namespace logging {

LogLevel globalLogLevel = INFO;

bool checkLogLevel(LogLevel level) {
    return level <= globalLogLevel;
}

std::string levelName(LogLevel level) {
    switch (level) {
        case ERROR: return "ERROR";
        case INFO:  return "INFO";
    }
    return "UNKNOWN";
}

std::string baseName(const char *path) {
    const char *slash = std::strrchr(path, '/');
    return slash ? slash + 1 : path;
}

std::string utcTimestamp() {
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

void logImpl(LogLevel level, const char *file, int line, const std::string &message) {
    if (!checkLogLevel(level)) {
        return;
    }
    std::ostream &os = (level == ERROR) ? std::cerr : std::cout;
    os << '[' << levelName(level) << ']'
       << '[' << utcTimestamp() << ']'
       << '[' << baseName(file) << ":" << line << ']' << " "
       << message << '\n';
}

}; // logging