#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unordered_map>
#include <thread>
#include <sys/epoll.h>
#include "codec.hpp"
#include "headers.hpp"
#include "bodies.hpp"
#include "response.hpp"
#include <fcntl.h>
#include "connection_handler.hpp"
#include "safe_queue.hpp"
#include "worker.hpp"
#include "metadata.hpp"

void set_nonblocking(int sockfd){
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl");
        exit(EXIT_FAILURE);
    }
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl");
        exit(EXIT_FAILURE);
    }
}

#include <sys/resource.h>

void maximize_fd_limit() {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

std::string server_log_dir = "/tmp/kraft-combined-logs";

int main(int argc, char* argv[]) {
    maximize_fd_limit();
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::string filepath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    parse_metadata_file(filepath);
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 4096;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";
    std::vector<worker> workers(4);
    for(int i = 0; i < 4; i++){
        int epoll_fd = epoll_create1(0);
        if (epoll_fd == -1) {
            perror("epoll_create1");
            exit(EXIT_FAILURE);
        }
        workers[i].epoll_fd = epoll_fd;
        workers[i].thread = std::thread(worker_run, epoll_fd);
    }
    int num_io_workers = 4;
    std::vector<std::thread> io_threads;
    for (int i = 0; i < num_io_workers; i++) {
        io_threads.emplace_back(io_worker_loop);
    }
    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);
    int total_clients = 0;
    while(true){
        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
        if(client_fd < 0) continue;
        set_nonblocking(client_fd);
        ConnectionState* state = new ConnectionState();
        state->client_fd = client_fd;
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.ptr = state;
        int worker_idx = total_clients % 4;
        state->worker_fd = workers[worker_idx].epoll_fd;
        epoll_ctl(workers[worker_idx].epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
        total_clients++;
    }
    close(server_fd);
    return 0;
}