#pragma once
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <cstdint>
#include <vector>

enum class ClientState{
    reading_size,
    reading_body,
    processing,
    writing
};

class ConnectionState{
public:
    int client_fd;
    int worker_fd;
    ClientState state;
    size_t expected_size;
    std::vector<uint8_t> in_buf;
    std::vector<uint8_t> out_buf;
    size_t out_offset;
    ConnectionState(){
        state = ClientState::reading_size;
        expected_size = 0;
        out_offset = 0;
    }
    ConnectionState(int client_fd) : client_fd(client_fd){
        state = ClientState::reading_size;
        expected_size = 0;
        out_offset = 0;
    }
};

class worker{
public:
    std::thread thread;
    int epoll_fd;
};