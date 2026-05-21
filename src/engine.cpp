#include <thread>
#include <event2/event.h>
#include "engine.hpp"

Engine::Engine()
    : connectionIdGen(0)
{
    Log(INFO, "initialising engine");
    eventBase = event_base_new();
    std::thread(eventLoop);
}

void Engine::eventLoop() {
    event_base_dispatch(eventBase);
}

Engine& Engine::getInstance() {
    static Engine engine;
    return engine;
}

//////////////////////////////////////////////////////////////////
// open/read/write/close API - called by TcpConn for given `cId`
//////////////////////////////////////////////////////////////////

int64_t Engine::open(const std::string &srcIp, 
                int srcPort, 
                const std::string &destIp,
                int destPort,
                ConnType connType) 
{

}

void Engine::read(int64_t cId, int n, std::vector<uint8_t> &buffer) {

}

void Engine::write(int64_t cId, int n, std::vector<uint8_t> &buffer) {

}

void Engine::close(int64_t cId) {

}