#include <memory>

#include "tcp_conn.hpp"
#include "engine.hpp"

TcpConn::TcpConn(int64_t cId)
    : connectionId(cId) {}

TcpConn* TcpConn::open(const std::string &srcIp, 
                       int srcPort, 
                       const std::string &destIp,
                       int destPort,
                       ConnType connType) 
{
    int64_t cId = Engine::getInstance().open(srcIp, srcPort, destIp, destPort, connType);
    if (cId == -1) {
        return nullptr;
    }
    TcpConn* conn = new TcpConn(cId);
    return conn;
}

void TcpConn::read(int n, std::vector<uint8_t> &buffer) {
    Engine::getInstance().read(connectionId, n, buffer);
}

void TcpConn::write(int n, std::vector<uint8_t> &buffer) {
    Engine::getInstance().write(connectionId, n, buffer);

}

void TcpConn::close() {
    Engine::getInstance().close(connectionId);
}