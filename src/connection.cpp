#include "connection.hpp"
#include "event_loop.hpp"
#include "utils.hpp"

connection::connection(const std::string &src_ip,
                       const std::string &dest_ip,
                       uint32_t src_port,
                       uint32_t dest_port,
                       struct event_base *event_base,
                       conn_type ct)
{
    src_ip_ = src_ip;
    dest_ip_ = dest_ip;
    src_port_ = src_port;
    dest_port_ = dest_port;

    event_base_ = event_base;

    udp_socket_fd_ = net::create_udp_socket(src_ip, dest_ip, src_port, dest_port);
    if (udp_socket_fd_ == -1) {
        reset();
        return;
    }

    send_stream_ = new send_stream(SEND_BUFFER_CAPACITY);
    recv_stream_ = new recv_stream(RECV_BUFFER_CAPACITY);

    if (ct == conn_type::CONNECT) {
        state_ = tcp_state::CLOSED;
    }
    else if (ct == conn_type::LISTEN) {
        state_ = tcp_state::LISTEN;
    }
}

connection::~connection() {
    // In most cases, connection would have proactively destroyed itself,
    // so this call is just-in-case. destroy() is (and should always remain) idempotent, so
    // calling multiple times should have no side effects.
    destroy();
}

int64_t connection::open() {
    if (state_ == tcp_state::CLOSED) {
        // send our iss (handshake syn)
        send_stream_->send_syn();
        state_ = tcp_state::SYN_SENT;
    }
    else if (state_ == tcp_state::LISTEN) {
        // do nothing
    }
    else {
        Log(level::ERROR, std::format("open() in bad state"));
        return -1;
    }

    // add recv_segment event to event loop
    struct event *recv_segment_ev = event_new(event_base_, 
                                              udp_socket_fd_, 
                                              EV_READ|EV_PERSIST, 
                                              libevent_on_recv_segment, 
                                              (void*)this);
    if (recv_segment_ev == NULL) {
        Log(level::ERROR, "failed to create recv_segment_ev event");
        return -1;
    }
    if (event_add(recv_segment_ev, NULL) < 0) {
        Log(level::ERROR, "failed to add recv_segment_ev event");
        return -1;
    }

    // block until move out of handshake state
    std::unique_lock<std::mutex> ul(pending_open_mtx_);
    pending_open_cv_.wait(ul, [&] {
        return (
            state_ != tcp_state::LISTEN &&
            state_ != tcp_state::SYN_SENT &&
            state_ != tcp_state::SYN_RECEIVED
        );
    });

    // wake up
    if (state_ != tcp_state::ESTABLISHED) {
        if (state_ == tcp_state::CLOSED) {
            // already torndown - do nothing
        }
        else {
            reset();
            return -1;
        }
    }

    return 0;
}

int64_t connection::read(uint64_t n, uint8_t *dest_buffer) {
    if (n == 0) return 0;

    if (dest_buffer == nullptr) {
        Log(level::ERROR, "read() dest_buffer ptr is null");
        return -1;
    }

    uint64_t ready = recv_stream_->ready_bytes();
    if (ready < n) {
        // block until enough bytes ready to read
        std::unique_lock<std::mutex> ul(pending_read_mtx_);
        pending_read_cv_.wait(ul, [&] {
            uint64_t ready = recv_stream_->ready_bytes();
            return ready < n;
        });
    }

    int64_t bytes_read = recv_stream_->read(n, dest_buffer);
    if (bytes_read != n) {
        Log(level::ERROR, std::format("{} bytes ready, but only {} bytes actually read", ready, bytes_read));
        return -1;
    }
    return bytes_read;
}

int64_t connection::write(uint64_t n, uint8_t *src_buffer) {
    if (n == 0) return 0;

    if (src_buffer == nullptr) {
        Log(level::ERROR, "write() dest_buffer ptr is null");
        return -1;
    }

    int64_t free_space = send_stream_->free_space_bytes();
    if (free_space < n) {
        Log(level::ERROR, std::format("insufficient space for write() - free_space={} < n={}", free_space, n));
        return -1;
    }

    int64_t bytes_written = send_stream_->write(n, src_buffer);
    if (bytes_written < 0) {
        Log(level::ERROR, "write() error");
        return -1;
    }
    return bytes_written;
}

int64_t connection::close() {
    return 0;
}

void connection::on_recv_segment() 
{
    uint32_t max_datagram_size = MSS + sizeof(tcp_header);
    uint8_t segment[max_datagram_size];
    int bytes_read = recv(udp_socket_fd_, segment, max_datagram_size, 0);
    if (bytes_read == -1) {
        return;
    }

    tcp_header hdr = (tcp_header)*segment;
    uint8_t *payload_ptr = segment + sizeof(hdr);
    uint64_t payload_size = bytes_read - sizeof(hdr);

    bool hdr_valid = hdr.src_port == dest_port_ && 
                     hdr.dest_port == src_port_ && 
                     net::tcp_checksum_valid(hdr, payload_ptr, payload_size);
    if (!hdr_valid) {
        // drop
        return;
    }

    if (hdr.rst()) {
        // regardless of the state, receiving rst makes us tear down
        reset();
        return;
    }

    switch (state_) {
        case tcp_state::CLOSED: {
            reset();
            return;
        } 
        break;

        case tcp_state::LISTEN: {
            if (hdr.fin()) {
                reset();
                return;
            }

            if (hdr.syn() && !hdr.ack()) {
                // syn received => send syn-ack
                recv_stream_->on_syn(hdr.seqnum);   // accept peer's iss
                send_stream_->send_syn_ack();       // send our iss + ack peer iss (handshake syn-ack)

                state_ = tcp_state::SYN_RECEIVED;
            }
            else {
                return;
            }
        } 
        break;

        case tcp_state::SYN_SENT: {
            if (hdr.fin()) {
                reset();
                return;
            }

            if (hdr.syn() && hdr.ack()) {
                // syn-ack received => send ack
                recv_stream_->on_syn(hdr.seqnum);   // accept peer's iss
                send_stream_->on_ack(hdr.acknum);   // accept peer's ack of our iss
                send_stream_->send_ack();           // ack peer's iss (handshake ack)

                state_ = tcp_state::ESTABLISHED;
            } 
            else {
                return;
            }
        } 
        break;

        case tcp_state::SYN_RECEIVED: {
            if (hdr.fin()) {
                reset();
                return;
            }

            if(!hdr.syn() && hdr.ack()) {
                // ack received
                state_ = tcp_state::ESTABLISHED;
            } 
            else {
                return;
            }
        } 
        break;

        case tcp_state::ESTABLISHED: {
            if (hdr.syn()) {
                // syn invalid in ESTABLISHED state
                reset();
                return;
            }

            if (hdr.ack()) {
                send_stream_->on_ack(hdr.acknum);
            }

            recv_stream_->recv_segment(hdr.seqnum, payload_ptr, payload_size);

            if (hdr.fin()) {
                recv_stream_->on_fin(hdr.seqnum + payload_size);
            }
        } 
        break;

        case tcp_state::FIN_WAIT_1: {

        } 
        break;

        case tcp_state::CLOSE_WAIT: {

        } 
        break;

        case tcp_state::FIN_WAIT_2: {

        } 
        break;

        case tcp_state::TIME_WAIT: {

        } 
        break;

        case tcp_state::LAST_ACK: {

        } 
        break;

        default: {
            throw std::runtime_error("unknown tcp_state reached");
        }
    }
}

void connection::reset() {
    // send rst
    send_stream_->send_rst();

    // teardown own connection immediately
    destroy();
}

void connection::destroy() {
    //
    // cleanup all our state
    //
    
    //
    // Notify owning connection_manager we're destroying connection, so it can
    // remove connection from the active list.
    //

    //
    // example
    //

    // state_ = tcp_state::CLOSED;
    // if (recv_segment_ev_) {
    //     event_del(recv_segment_ev_);
    //     event_free(recv_segment_ev_);
    //     recv_segment_ev_ = nullptr;
    // }
    // if (timeout_ev_) {
    //     event_del(timeout_ev_);
    //     event_free(timeout_ev_);
    //     timeout_ev_ = nullptr;
    // }
    // if (udp_socket_fd_ != -1) {
    //     close(udp_socket_fd_);
    //     udp_socket_fd_ = -1;
    // }
    // delete send_stream_;
    // send_stream_ = nullptr;
    // delete recv_stream_;
    // recv_stream_ = nullptr;

    // then, call: manager.on_destroy(id)
    // manager will then erase unique_ptr from active list
    // destroy() probably runs again from destructor, but its
    // idempotent, so all good

}
