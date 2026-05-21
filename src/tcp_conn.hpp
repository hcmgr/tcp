#pragma once
#include <string>
#include <vector>
#include <memory>

enum class ConnType {
    LISTEN,
    CONNECT
};

class TcpConn {
public:
    /**
     * Establish a TCP connection with the 4-tuple: (`srcIp`, `srcPort`, `destIp`, `destPort`).
     * `connType` is either:
     *      - LISTEN ==> open a connection, wait for peer to connect
     *      - CONNECT ==> connect to already-listening peer
     * All other `connType`'s are invalid.
     * 
     * Open is a blocking call.
     */
    static TcpConn* open(const std::string &srcIp, 
                                         int srcPort, 
                                         const std::string &destIp,
                                         int destPort,
                                         ConnType connType);

    /**
     * Read `n` bytes from stream into `buffer`.
     * Blocking call, waits until `n` bytes available.
     */
    void read(int n, std::vector<uint8_t> &buffer);

    /**
     * Write `n` bytes from `buffer` into stream.
     * Non-blocking call.
     */
    void write(int n, std::vector<uint8_t> &buffer);

    /**
     * Close stream.
     */
    void close();

private:
    TcpConn(int64_t cId);

private:
    int64_t connectionId;
};