#include <deque>
#include <memory>
#include <sstream>

#include "buffer.hpp"
#include "define.hpp"
#include "buffer.hpp"

class recv_stream {
private:
    // logical seqnum state
    uint64_t irs_;
    uint64_t nxt_;
    uint64_t fin_;

    // physical buffer offsets
    uint64_t nxt_pos_;
    uint64_t rd_pos_;

    // physical buffer
    std::unique_ptr<ring_buffer> buffer_;

    struct segment {
        uint64_t seqnum;
        uint64_t payload_len;
        uint64_t payload_pos;   // position in physical buffer of first byte
    };

    // received segments non-contiguous with nxt
    std::deque<segment> pending_segments_;

    enum class state {
        SYN_WAITING,
        ESTABLISHED,
        FINISHED
    };
    state state_;

public:
    recv_stream(uint64_t capacity);
    ~recv_stream();

public:
    // receive segment payload from peer
    int64_t recv_segment(uint64_t seqnum, uint8_t *payload_ptr, uint64_t payload_size);

    // user read for `n` available bytes
    int64_t read(uint64_t n, uint8_t *dest_buffer);

    // on peer's syn/fin
    int64_t on_syn_recv(uint64_t irs);
    int64_t on_fin_recv(uint64_t fin);

public:
    uint64_t get_num_ready_bytes();
    uint64_t get_num_free_space_bytes();
    uint64_t get_nxt() { return nxt_; }

    bool is_finished() { return state_ == state::FINISHED; }

    std::string to_string();

private:
    uint64_t inc(uint64_t pos, uint64_t n) const { return (pos + n) % buffer_->capacity(); }

    static std::string to_string(state s) {
        switch (s) {
            case state::SYN_WAITING: return "SYN_WAITING";
            case state::ESTABLISHED: return "ESTABLISHED";
            case state::FINISHED:    return "FINISHED";
        }
        return "UNKNOWN";
    }
};