#include "connection.hpp"
#include "event_loop.hpp"
#include "manager.hpp"
#include "utils.hpp"

connection::connection(uint64_t id, const addr_tuple &addr_tuple)
{
    // Constructor is intentionally thin - we don't want to initialise the 'heavy' state until 
    // open() called (udp socket, recv_segment event, send/recv streams, etc).
    id_ = id;
    addr_tuple_ = addr_tuple;
    recv_segment_ev_ = nullptr;
    state_ = tcp_state::CLOSED;
}

connection::~connection() {
    // In all cases, connection should proactively destroy itself, and will notify
    // owning manager its destroyed. This is just in case.
    if (state_ != tcp_state::CLOSED) {
        Log(level::ERROR, "destroy() called from destructor - this late indicates something's wrong");
        destroy();
    }
}

int64_t connection::open(const conn_type &conn_type) {
    if (state_ != tcp_state::CLOSED) {
        Log(level::ERROR, std::format("open() in bad state"));
        return -1;
    }

    // create udp socket
    udp_socket_fd_ = net::create_udp_socket(addr_tuple_.src_ip_, addr_tuple_.dest_ip_, addr_tuple_.src_port_, addr_tuple_.dest_port_);
    if (udp_socket_fd_ == -1) {
        goto fail;
    }

    // add recv_segment event to event loop
    {
        recv_segment_ev_ = event_new(manager::get_instance().get_event_base(),
                                    udp_socket_fd_,
                                    EV_READ|EV_PERSIST,
                                    libevent_on_recv_segment,
                                    (void*)this);
        if (recv_segment_ev_ == NULL) {
            Log(level::ERROR, "failed to create recv_segment_ev event");
            goto fail;
        }
        if (event_add(recv_segment_ev_, NULL) < 0) {
            Log(level::ERROR, "failed to add recv_segment_ev event");
            goto fail;
        }
    }

    // create send and recv streams
    {
        auto send_segment_cb = [this](uint64_t seqnum, uint16_t flags, uint8_t *payload_ptr, uint64_t payload_len) {
            return send_segment(seqnum, flags, payload_ptr, payload_len);
        };
        send_stream_ = std::make_unique<send_stream>(SEND_BUFFER_CAPACITY, send_segment_cb);
        recv_stream_ = std::make_unique<recv_stream>(RECV_BUFFER_CAPACITY);
    }

    // create delayed ack timeout handler
    delayed_ack_timeout_ = std::make_unique<timeout_handler>(
        DELAYED_ACK_TIMEOUT_MS,
        libevent_on_delayed_ack_timeout,
        this);

    // initialise connection as either connect or listen
    switch (conn_type) {
        case conn_type::CONNECT: {
            // send our iss (handshake syn)
            send_syn();
            state_ = tcp_state::SYN_SENT;
        } break;

        case conn_type::LISTEN: {
            // do nothing - wait for peer syn
            state_ = tcp_state::LISTEN;
        } break;

        default: {
            throw std::runtime_error("unknown conn_type");
        }
    }

    // block until move out of handshake state
    {
        std::unique_lock<std::mutex> ul(pending_open_mtx_);
        pending_open_cv_.wait(ul, [&] {
            return (
                state_ != tcp_state::LISTEN &&
                state_ != tcp_state::SYN_SENT &&
                state_ != tcp_state::SYN_RECEIVED
            );
        });
    }

    // wake up - teardown if haven't entered ESTABLISHED state
    if (state_ != tcp_state::ESTABLISHED) {
        goto fail;
    }

    return 0;

fail:
    reset();
    return -1;
}

int64_t connection::read(uint64_t n, uint8_t *dest_buffer) {
    if (n == 0) return 0;

    if (dest_buffer == nullptr) {
        Log(level::ERROR, "read() dest_buffer ptr is null");
        return -1;
    }

    if (state_ != tcp_state::ESTABLISHED) {
        Log(level::ERROR, std::format("read() in non-ESTABLISHED state is invalid - {}", to_string(state_)));
        return -1;
    }

    uint64_t ready = recv_stream_->get_num_ready_bytes();
    if (ready < n) {
        // block until enough bytes ready to read
        std::unique_lock<std::mutex> ul(pending_read_mtx_);
        pending_read_cv_.wait(ul, [&] {
            uint64_t ready = recv_stream_->get_num_ready_bytes();
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

    if (!(state_ == tcp_state::ESTABLISHED || state_ == tcp_state::CLOSE_WAIT)) {
        Log(level::ERROR, std::format("write() in invalid state - {}", to_string(state_)));
        return -1;
    }

    int64_t bytes_written = send_stream_->write(n, src_buffer);
    if (bytes_written < 0) {
        Log(level::ERROR, "write() error");
        return -1;
    }
    return bytes_written;
}

int64_t connection::shutdown() {
    if (!(state_ == tcp_state::ESTABLISHED || state_ == tcp_state::CLOSE_WAIT)) {
        Log(level::ERROR, std::format("close() in invalid state - {}", to_string(state_)));
        reset();
        return -1;
    }

    auto res = send_fin();
    if (res < 0) {
        reset();
        return -1;
    }

    if (state_ == tcp_state::ESTABLISHED) {
        state_ = tcp_state::FIN_WAIT_1;
    }
    else if (state_ == tcp_state::CLOSE_WAIT) {
        state_ = tcp_state::LAST_ACK;
    } 
    else {
        throw std::runtime_error(std::format("close() - unreachable state - {}", state_));
    }

    return 0;
}

int64_t connection::send_segment(uint64_t seqnum, uint16_t flags, uint8_t *payload_ptr, uint64_t payload_len) {
    tcp_header hdr = make_header(seqnum, flags, payload_ptr, payload_len);

    uint32_t to_send_bytes = sizeof(tcp_header) + payload_len;
    uint8_t buf[to_send_bytes];
    std::memcpy(buf, &hdr, sizeof(tcp_header));
    if (payload_len > 0) {
        std::memcpy(buf + sizeof(tcp_header), payload_ptr, payload_len);
    }

    // non-blocking udp send - should succeed immediately
    int64_t sent_bytes = send(udp_socket_fd_, buf, to_send_bytes, 0);
    if (sent_bytes < 0) {
        Log(level::ERROR, std::format("error on send() - {}, segment - {}", strerror(errno), hdr.to_string()));
        return -1;
    }
    if (sent_bytes != to_send_bytes) {
        Log(level::ERROR, std::format("sent_bytes ({}) != to_send_bytes ({}) - ", sent_bytes, to_send_bytes));
        return -1;
    }

    // cancel current delayed-ack (if any) - implictly piggyback'd on this segment
    if ((flags & ack_mask) && delayed_ack_timeout_->active) {
        delayed_ack_timeout_->clear();
    }

    return sent_bytes;
}

void connection::on_recv_segment()
{
    uint32_t max_datagram_size = MSS + sizeof(tcp_header);
    uint8_t segment[max_datagram_size];
    uint32_t bytes_read = recv(udp_socket_fd_, segment, max_datagram_size, 0);
    if (bytes_read == -1) {
        Log(level::ERROR, std::format("recv() error - {}", strerror(errno)));
        return;
    }

    tcp_header hdr = (tcp_header)*segment;
    uint8_t *payload_ptr = segment + sizeof(hdr);
    uint64_t payload_len = bytes_read - sizeof(hdr);

    bool hdr_valid = hdr.src_port == addr_tuple_.dest_port_ &&
                     hdr.dest_port == addr_tuple_.src_port_ &&
                     net::tcp_checksum_valid(hdr, payload_ptr, payload_len);
    if (!hdr_valid) {
        Log(level::ERROR, std::format("tcp hdr invalid - tearing down - {}", hdr.to_string()));
        goto fail;
    }

    // regardless of the state, receiving rst makes us tear down
    if (hdr.rst()) {
        Log(level::INFO, "received rst - tearing down");
        goto fail;
    }

    send_stream_->set_peer_recv_window(hdr.window);

    //
    // tcp state machine
    //
    switch (state_) {
        case tcp_state::CLOSED: {
            Log(level::ERROR, "in CLOSED state - received segment - invalid");
            goto fail;
        }
        break;

        case tcp_state::LISTEN: {
            // handshake syn received
            if (hdr.syn() && !hdr.ack() && !hdr.fin()) {
                // accept peer's iss
                auto res = recv_stream_->on_syn_recv(hdr.seqnum);
                if (res < 0) {
                    goto fail;
                }

                // send our iss + ack peer's iss (handshake syn-ack send)
                res = send_syn_ack();
                if (res < 0) {
                    goto fail;
                }

                state_ = tcp_state::SYN_RECEIVED;
            }
            else {
                Log(level::ERROR, std::format("in LISTEN state - received something other than syn segment - invalid - {}", hdr.to_string()));
                goto fail;
            }
        }
        break;

        case tcp_state::SYN_SENT: {
            // handshake syn-ack received
            if (hdr.syn() && hdr.ack() && !hdr.fin()) {
                // accept peer's iss
                auto res = recv_stream_->on_syn_recv(hdr.seqnum);
                if (res < 0) {
                    goto fail;
                }
                
                // accept peer's ack of our iss
                res = send_stream_->on_ack_recv(hdr.acknum);
                if (res < 0) {
                    goto fail;
                }

                // ack peer's iss (handshake ack send)
                res = send_ack();
                if (res < 0) {
                    goto fail;
                }

                state_ = tcp_state::ESTABLISHED;
            }
            else {
                Log(level::ERROR, std::format("in SYN_SENT state - received something other than syn-ack - invalid - {}", hdr.to_string()));
                goto fail;
            }
        }
        break;

        case tcp_state::SYN_RECEIVED: {
            // handshake ack received
            if(!hdr.syn() && hdr.ack() && !hdr.fin()) {
                state_ = tcp_state::ESTABLISHED;
            }
            else {
                Log(level::ERROR, std::format("in SYN_RECEIVED state - received something other than ack - invalid - {}", hdr.to_string()));
                goto fail;
            }
        }
        break;

        case tcp_state::ESTABLISHED: {
            // process segment
            auto res = process_segment_established(hdr, payload_ptr, payload_len);
            if (res < 0) {
                goto fail;
            }

            if (recv_stream_->is_finished()) {
                // received their fin - now wait to send our fin
                state_ = tcp_state::CLOSE_WAIT;
            } else {
                // continue as normal
                state_ = tcp_state::ESTABLISHED;
            }
        }
        break;

        case tcp_state::FIN_WAIT_1: {
            // process segment
            auto res = process_segment_established(hdr, payload_ptr, payload_len);
            if (res < 0) {
                goto fail;
            }

            bool send_finished = send_stream_->is_finished();
            bool recv_finished = recv_stream_->is_finished();
            if (!send_finished && !recv_finished) {
                // still waiting for their fin AND ack of our fin
                state_ = tcp_state::FIN_WAIT_1;
            }
            else if (!send_finished && recv_finished) {
                // still waiting for ack of our fin
                state_ = tcp_state::CLOSING;
            }
            else if (send_finished && !recv_finished) {
                // still waiting for their fin
                state_ = tcp_state::FIN_WAIT_2;
            } else {
                // their fin received, our fin ack'd => close immediately
                destroy();
            }
        }
        break;

        case tcp_state::CLOSE_WAIT: {
            Log(level::ERROR, "receiving segment in CLOSE_WAIT state is invalid");
            goto fail;
        }
        break;

        case tcp_state::FIN_WAIT_2: {
            bool recv_finished = recv_stream_->is_finished();
            if (recv_finished) {
                // have received their fin, and sent ack for it - enter time-wait
                state_ = tcp_state::TIME_WAIT;
            }
            else {
                // keep waiting for their fin
                state_ = tcp_state::FIN_WAIT_2;
            }
        }
        break;

        case tcp_state::TIME_WAIT: {
            // During TIME_WAIT state, we're waiting TIME_WAIT_MS to close.
            // If we receive a segment, it's cause our ack of their fin didn't
            // arrive, and they have re-tx'd it. All other cases are invalid.
            if (!(hdr.fin() && !hdr.syn() && !hdr.ack())) {
                Log(level::ERROR, std::format("in TIME_WAIT state - segment other than pure fin arrive - invalid - {}", hdr.to_string()));
                goto fail;
            }

            // we've already received their fin - verify retx'd one == first one
            uint64_t retx_fin = hdr.seqnum + payload_len;
            uint64_t fin = recv_stream_->get_nxt();
            if (retx_fin != fin) {
                Log(level::ERROR, std::format("in TIME_WAIT STATE - re-tx'd fin {} != first fin {}", retx_fin, fin));
                goto fail;
            }

            // Ack the fin. 
            // Just send immediately, i.e. don't use a delayed-ack - we have
            // no more acks after this, so not point buffering.
            if (delayed_ack_timeout_->active) {
                delayed_ack_timeout_->clear();
                auto res = send_ack();
                if (res < 0) {
                    goto fail;
                }
            } 

            state_ = tcp_state::TIME_WAIT;
        }
        break;

        case tcp_state::LAST_ACK: {
            // should be receiving peer's ack of our fin (nothing else)
            if (!(hdr.fin() && !hdr.syn() && !hdr.ack())) {
                goto fail;
            }

            auto res = send_stream_->on_ack_recv(hdr.acknum);
            if (res < 0) {
                goto fail;
            }

            if (!send_stream_->is_finished()) {
                Log(level::ERROR, "in CLOSING state, yet peer didn't ack our fin - invalid");
                goto fail;
            }

            destroy();
        }
        break;
    
        case tcp_state::CLOSING: {
            // should be receiving peer's ack of our fin (nothing else)
            if (!(hdr.fin() && !hdr.syn() && !hdr.ack())) {
                goto fail;
            }

            auto res = send_stream_->on_ack_recv(hdr.acknum);
            if (res < 0) {
                goto fail;
            }

            if (!send_stream_->is_finished()) {
                Log(level::ERROR, "in CLOSING state, yet peer didn't ack our fin - invalid");
                goto fail;
            }

            state_ = tcp_state::TIME_WAIT;
        } 
        break;

        default: {
            goto fail;
        }
    };
    return;

fail:
    reset();
    return;
}

void connection::on_delayed_ack_timeout() {
    if (!delayed_ack_timeout_->active) {
        Log(level::ERROR, "delayed-ack-timeout triggered whilst its inactive");
        reset();
        return;
    }

    // send ack with whatever recv_stream's latest acknum is
    auto res = send_ack();
    if (res < 0) {
        reset();
        return;
    }

    delayed_ack_timeout_->clear();
}

void connection::destroy() {
    // remove and free recv segment event
    event_free(recv_segment_ev_);
    recv_segment_ev_ = nullptr;

    // close udp socket
    auto res = close(udp_socket_fd_);
    if (res < 0) {
        // not much else we can do here, as we're CURRENTLY destroying - sucks to suck
        throw std::runtime_error(std::format("error closing udp socket fd - {}", strerror(errno)));
    }

    //
    // rest of the state will free on RAII
    //

    // move to closed state, and notify manager we validly closed
    state_ = tcp_state::CLOSED;
    manager::get_instance().on_connection_destroy(id_);
}

void connection::reset() {
    // send rst
    send_rst();

    // teardown own connection immediately
    destroy();
}

tcp_header connection::make_header(uint32_t seqnum, 
                                   uint16_t flags,
                                   uint8_t *payload_ptr,
                                   uint64_t payload_len)
{
    tcp_header hdr;

    hdr.src_port = addr_tuple_.src_port_;
    hdr.dest_port = addr_tuple_.dest_port_;
    hdr.seqnum = seqnum;
    hdr.acknum = recv_stream_->get_nxt();
    hdr.flags = flags;
    hdr.window = recv_stream_->get_num_free_space_bytes();
    hdr.checksum = net::tcp_checksum_calc(hdr, payload_ptr, payload_len);

    return hdr;
}

int64_t connection::process_segment_established(tcp_header &hdr,
                                                uint8_t *payload_ptr,
                                                uint64_t payload_len) 
{
    if (hdr.syn()) {
        return -1;
    }

    // accept ack
    if (hdr.ack()) {
        auto res = send_stream_->on_ack_recv(hdr.acknum);
        if (res < 0) {
            return -1;
        }
    }

    // accept segment payload
    uint64_t curr_acknum = recv_stream_->get_nxt();
    if (payload_len > 0) {
        auto res = recv_stream_->recv_segment(hdr.seqnum, payload_ptr, payload_len);
        if (res < 0) {
            return -1;
        }
    }

    // accept fin - recv_stream will bump acknum so our ack fires
    if (hdr.fin()) {
        auto res = recv_stream_->on_fin_recv(hdr.seqnum + payload_len);
        if (res < 0) {
            return -1;
        }
    }

    // ack newly-received bytes
    uint64_t new_acknum = recv_stream_->get_nxt();
    if (new_acknum < curr_acknum) {
        return -1;
    } 
    if (new_acknum > curr_acknum) {
        //
        // don't queue delayed-ack if:
        //      a) this is our last ack (i.e. recv finished because peer sent fin), OR;
        //      b) there's a delayed ack already active (just cancel it and send this one)
        //
        bool recv_finished = recv_stream_->is_finished();
        bool should_delay_ack = !recv_finished && !delayed_ack_timeout_->active;
        if (should_delay_ack) {
            auto res = delayed_ack_timeout_->add();
            if (res < 0) {
                return -1;
            }
        }
        else {
            // cancel current one (if any)
            delayed_ack_timeout_->clear();
            auto res = send_ack();
            if (res < 0) {
                return -1;
            }
        }
    }

    return 0;
}

//
// for our special sends:
//      syn/syn_ack/fin all consume a seqnum, and are thus routed through send_stream.
//      ack/rst consume no seqnum, so we can send from connection immediately.
//

int64_t connection::send_syn() {
    return send_stream_->send_syn();
}

int64_t connection::send_syn_ack() {
    return send_stream_->send_syn_ack();
}

int64_t connection::send_fin() {
    return send_stream_->send_fin();
}

int64_t connection::send_ack() {
    uint64_t seqnum = 0;
    uint16_t flags = ack_mask;
    return send_segment(seqnum, flags, nullptr, 0);
}

int64_t connection::send_rst() {
    uint64_t seqnum = 0;
    uint16_t flags = rst_mask;
    return send_segment(seqnum, flags, nullptr, 0);
}
