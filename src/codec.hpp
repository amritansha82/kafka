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
#include <vector>
#include <cstdint>

class Message{
private:
public:
    std::vector<uint8_t> payload;
    Message() = default;
    void append64(int64_t value);
    void append32(int32_t value);
    void append16(int16_t value);
    void append8(int8_t value);
    void append(const char* data, size_t size);
    void append_varint(uint32_t value);
    std::vector<uint8_t> to_vec();
};

std::vector<uint8_t> receive_message(int client_fd);

Message parse_message(int client_fd);

uint32_t read_varint(const Message& msg, size_t& offset);
uint64_t read_varint(const std::vector<uint8_t>& buffer, size_t& offset);
uint64_t read_zigzag_varint(const std::vector<uint8_t>& buffer, size_t& offset);
int32_t read_32_be(const std::vector<uint8_t>& buffer, size_t& offset);