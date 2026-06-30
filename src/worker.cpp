#include "worker.hpp"
#include "response.hpp"
void worker_run(int epoll_fd){
    struct epoll_event events[128];
    while(true){
        int num_events = epoll_wait(epoll_fd, events, 128, -1);
        for(int i = 0; i < num_events; i++){
            ConnectionState* state = static_cast<ConnectionState*>(events[i].data.ptr);
            if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
                close(state->client_fd);
                delete state;
                continue;
            }
            if(events[i].events & EPOLLIN){
                while (true) {
                    uint8_t chunk[1024];
                    ssize_t bytes_read = recv(state->client_fd, chunk, sizeof(chunk), 0);
                    if (bytes_read < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        break;
                    } else if (bytes_read == 0) {
                        break;
                    }
                    state->in_buf.insert(state->in_buf.end(), chunk, chunk + bytes_read);
                }
                if (state->state == ClientState::reading_size) {
                    if (state->in_buf.size() >= 4) {
                        uint32_t network_size;
                        std::memcpy(&network_size, state->in_buf.data(), 4);
                        state->expected_size = ntohl(network_size);
                        state->state = ClientState::reading_body;
                    }
                }
                if (state->state == ClientState::reading_body) {
                    if (state->in_buf.size() >= 4 + state->expected_size) {
                        state->state = ClientState::processing;
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, state->client_fd, nullptr);
                        request_channel.push(state);
                        break;
                    }
                }
            }
            if (events[i].events & EPOLLOUT) {
                while(true){
                    ssize_t bytes_sent = send(state->client_fd, state->out_buf.data() + state->out_offset, state->out_buf.size() - state->out_offset, 0);
                    if (bytes_sent < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; 
                        }
                        break;
                    } else if (bytes_sent == 0) {
                        break; 
                    }
                    state->out_offset += bytes_sent;
                    if (state->out_offset == state->out_buf.size()) {
                        state->out_offset = 0;
                        state->out_buf.clear();
                        state->state = ClientState::reading_size;
                        struct epoll_event ev;
                        ev.events = EPOLLIN | EPOLLRDHUP;
                        ev.data.ptr = state;
                        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, state->client_fd, &ev);
                        break;
                    }
                }
            }
        }
    }
}

void io_worker_loop() {
    while (true) {
        ConnectionState* state = request_channel.pop();
        Message request_msg;
        request_msg.payload.assign(state->in_buf.begin() + 4, state->in_buf.end());
        Message response_msg = create_response(request_msg);
        state->out_buf = response_msg.to_vec();
        state->state = ClientState::writing;
        state->in_buf.clear(); 
        struct epoll_event ev;
        ev.events = EPOLLOUT | EPOLLRDHUP;
        ev.data.ptr = state;
        epoll_ctl(state->worker_fd, EPOLL_CTL_ADD, state->client_fd, &ev);
    }
}