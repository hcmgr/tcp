#include <cstdint>

class CongestionController {
private:
    int64_t cwnd;
public:
    CongestionController() {}
    ~CongestionController() {}
public:
    int64_t onAck() {}
    int64_t onTripleDupAck() {}
    int64_t onRtoTimeout() {}
};