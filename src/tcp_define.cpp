#include <string>

#include "tcp_define.hpp"

std::string toString(State state) {
    switch (state) {
        case State::CLOSED:        return "CLOSED";
        case State::LISTEN:        return "LISTEN";
        case State::SYN_SENT:      return "SYN_SENT";
        case State::SYN_RECEIVED:  return "SYN_RECEIVED";
        case State::ESTABLISHED:   return "ESTABLISHED";
        case State::FIN_WAIT_1:    return "FIN_WAIT_1";
        case State::CLOSE_WAIT:    return "CLOSE_WAIT";
        case State::FIN_WAIT_2:    return "FIN_WAIT_2";
        case State::TIME_WAIT:     return "TIME_WAIT";
        case State::LAST_ACK:      return "LAST_ACK";
    }
    return "UNKNOWN";
}