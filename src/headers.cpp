#include "headers.hpp"

request_header_v0::request_header_v0(const std::vector<uint8_t>& raw_data, size_t& offset){
    uint32_t network_val;
    std::memcpy(&network_val, raw_data.data() + offset, 4);
    correlation_id = ntohl(network_val);
    offset += 4;
}

request_header_v0::request_header_v0(int32_t correlation_id){
    this->correlation_id = correlation_id;
}

void request_header_v0::append_to(Message& msg){
    msg.append32(correlation_id);
}

request_header_v2::request_header_v2(const std::vector<uint8_t>& raw_data, size_t& offset){
    uint16_t req_api_key;
    std::memcpy(&req_api_key, raw_data.data() + offset, 2);
    api_key = ntohs(req_api_key);
    offset += 2;
    uint16_t req_api_version;
    std::memcpy(&req_api_version, raw_data.data() + offset, 2);
    api_version = ntohs(req_api_version);
    offset += 2;
    uint32_t network_val;
    std::memcpy(&network_val, raw_data.data() + offset, 4);
    correlation_id = ntohl(network_val);
    offset += 4;
    int16_t client_id_len;
    std::memcpy(&client_id_len, raw_data.data() + offset, 2);
    client_id_len = ntohs(client_id_len);
    offset += 2;
    if (client_id_len > 0){
        client_id = std::string(reinterpret_cast<const char*>(raw_data.data() + offset), client_id_len);
        offset += client_id_len;
    }
    else if(client_id_len == 0){
        client_id = "";
    }
    else if(client_id_len == -1){
        client_id = std::nullopt;
    }
    else{
    }
    uint8_t tag_count = raw_data[offset];
    offset += 1;
}

request_header_v2::request_header_v2(int16_t api_key, int16_t api_version, int32_t correlation_id, std::optional<std::string> client_id){
    this->api_key = api_key;
    this->api_version = api_version;
    this->correlation_id = correlation_id;
    this->client_id = client_id;
}

void request_header_v2::append_to(Message& msg){
    msg.append16(api_key);
    msg.append16(api_version);
    msg.append32(correlation_id);
    if (client_id){
        msg.append16(client_id->length());
        msg.append(client_id->data(), client_id->length());
    }
    else{
        msg.append16(-1);
    }
    msg.append8(0);
}

void response_header_v1::append_to(Message& msg) {
    msg.append32(correlation_id);
    msg.append8(0);
}