#pragma once
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <cstdint>
#include <vector>
#include "codec.hpp"

class request_header_v0{
public:
    int32_t correlation_id;
    request_header_v0(const std::vector<uint8_t>& raw_data, size_t& offset);
    request_header_v0(int32_t correlation_id);
    void append_to(Message& msg);
};

class request_header_v2{
public:
    int16_t api_key;
    int16_t api_version;
    int32_t correlation_id;
    std::optional<std::string> client_id;
    request_header_v2(const std::vector<uint8_t>& raw_data, size_t& offset);
    request_header_v2(int16_t api_key, int16_t api_version, int32_t correlation_id, std::optional<std::string> client_id);
    void append_to(Message& msg);
};

class response_header_v1 {
public:
    int32_t correlation_id;
    response_header_v1(int32_t correlation_id) : correlation_id(correlation_id) {}
    void append_to(Message& msg);
};