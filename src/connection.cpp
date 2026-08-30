#include "connection.hpp"
#include "event_loop.hpp"
#include "manager.hpp"
#include "utils.hpp"

connection::connection(uint64_t id, const addr_tuple &addr_tuple)
{
    //
    // Constructor is intentionally thin - we don't want to initialise the 'heavy' state until 
    // open() called (udp socket, recv_segment event, send/recv streams, etc).
    //
    id_ = id;
    addr_tuple_ = addr_tuple;
    state_ = tcp_state::CLOSED;
}

connection::~connection() {
    // In most cases, connection would have proactively destroyed itself,
    // so this call is just-in-case. destroy() is (and should always remain) idempotent, so
    // calling multiple times should have no side effects.
    destroy();
}

int64_t connection::open(const conn_type &conn_type) {
    if (state_ != tcp_state::CLOSED) {
        Log(level::ERROR, std::format("open() in bad state"));
        return -1;
    }

    // create udp socket
    udp_socket_fd_ = net::create_udp_socket(addr_tuple_.src_ip_, addr_tuple_.dest_ip_, addr_tuple_.src_port_, addr_tuple_.dest_port_);
    if (udp_socket_fd_ == -1) {
        destroy();
        return -1;
    }

    // add recv_segment event to event loop
    struct event *recv_segment_ev = event_new(manager::get_instance().get_event_base(),
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

    // create send and recv streams
    auto send_segment_cb = [this](uint64_t seqnum, uint16_t flags, uint8_t *payload_ptr, uint64_t payload_len) {
        return send_segment(seqnum, flags, payload_ptr, payload_len);
    };
    send_stream_ = std::make_unique<send_stream>(SEND_BUFFER_CAPACITY, send_segment_cb);
    recv_stream_ = std::make_unique<recv_stream>(RECV_BUFFER_CAPACITY);

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
    std::unique_lock<std::mutex> ul(pending_open_mtx_);
    pending_open_cv_.wait(ul, [&] {
        return (
            state_ != tcp_state::LISTEN &&
            state_ != tcp_state::SYN_SENT &&
            state_ != tcp_state::SYN_RECEIVED
        );
    });

    // wake up - teardown if haven't entered ESTABLISHED state
    if (state_ != tcp_state::ESTABLISHED) {
        reset();
        return -1;
    }

    return 0;
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

    if (state_ != tcp_state::ESTABLISHED) {
        Log(level::ERROR, std::format("write() in non-ESTABLISHED state is invalid - {}", to_string(state_)));
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

tcp_header connection::make_header(uint32_t seqnum, 
                                   uint16_t flags,
                                   uint8_t *payload_ptr,
                                   uint64_t payload_len)
{
    tcp_header hdr;

    hdr.src_port = addr_tuple_.src_port_;
    hdr.dest_port = addr_tuple_.dest_port_;
    hdr.seqnum = seqnum;
    hdr.acknum = recv_stream_->nxt();
    hdr.flags = flags;
    hdr.window = recv_stream_->free_space_bytes();
    hdr.checksum = net::tcp_checksum_calc(hdr, payload_ptr, payload_len);

    return hdr;
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
    uint64_t payload_size = bytes_read - sizeof(hdr);

    bool hdr_valid = hdr.src_port == addr_tuple_.dest_port_ &&
                     hdr.dest_port == addr_tuple_.src_port_ &&
                     net::tcp_checksum_valid(hdr, payload_ptr, payload_size);
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
            goto fail;
        }
        break;

        case tcp_state::LISTEN: {
            if (hdr.fin())
                goto fail;
            
            if (hdr.syn() && !hdr.ack()) {
                //
                // syn received
                //

                // accept peer's iss
                auto res = recv_stream_->on_syn_recv(hdr.seqnum);
                if (res < 0) {
                    goto fail;
                }

                // send our iss + ack peer's iss (handshake syn-ack)
                res = send_syn_ack();
                if (res < 0) {
                    goto fail;
                }

                state_ = tcp_state::SYN_RECEIVED;
            }
            else {
                return;
            }
        }
        break;

        case tcp_state::SYN_SENT: {
            if (hdr.fin()) {
                goto fail;
            }
            
            if (hdr.syn() && hdr.ack()) {
                //
                // syn-ack received
                //

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

                // ack peer's iss (handshake ack)
                res = send_ack();
                if (res < 0) {
                    goto fail;
                }

                state_ = tcp_state::ESTABLISHED;
            }
            else {
                return;
            }
        }
        break;

        case tcp_state::SYN_RECEIVED: {
            if (hdr.fin()) {
                goto fail;
            }

            if(!hdr.syn() && hdr.ack()) {
                //
                // ack received
                //
                state_ = tcp_state::ESTABLISHED;
            }
            else {
                return;
            }
        }
        break;

        case tcp_state::ESTABLISHED: {
            // syn invalid in ESTABLISHED state
            if (hdr.syn()) {
                goto fail;
            }

            // handle ack
            if (hdr.ack()) {
                auto res = send_stream_->on_ack_recv(hdr.acknum);
                if (res < 0) {
                    goto fail;
                }
            }

            // accept segment payload
            auto res = recv_stream_->recv_segment(hdr.seqnum, payload_ptr, payload_size);
            if (res < 0) {
                goto fail;
            }

            // ack newly received bytes
            if (delayed_ack_timeout_->active) {
                // delayed-ack queued - cancel it, and just send this one
                delayed_ack_timeout_->clear();
                res = send_ack();
                if (res < 0) {
                    goto fail;
                }
            } 
            else {
                // no current delayed-ack - queue one
                res = delayed_ack_timeout_->add();
                if (res < 0) {
                    goto fail;
                }
            }

            // handle fin
            if (hdr.fin()) {
                res = recv_stream_->on_fin_recv(hdr.seqnum + payload_size);
                if (res < 0) {
                    goto fail;
                }
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

void connection::reset() {
    // send rst
    send_rst();

    // teardown own connection immediately
    destroy();
}

void connection::destroy() {
    //
    // cleanup all our state
    //
    
    //
    // Notify owning manager we're destroying connection, so it can
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

    state_ = tcp_state::DESTROYED;
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
