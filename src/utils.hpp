#pragma once
#include <string>

//////////////////////////////////////////////////////////////////
// Logging
//////////////////////////////////////////////////////////////////

enum LogLevel {
    ERROR,
    INFO
};
#define Log(level, message) logging::logImpl((level), __FILE__, __LINE__, (message))

namespace logging {
    void logImpl(LogLevel level, const char *file, int line, const std::string &message);
};
