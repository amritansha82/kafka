#include "codec.hpp"

void Message::append64(int64_t value){
    uint64_t v = static_cast<uint64_t>(value);
    for (int i = 56; i >= 0; i -= 8) {
        payload.push_back(static_cast<uint8_t>((v >> i) & 0xFF));
    }
}

void Message::append32(int32_t value){
    uint32_t network_value = htonl(value);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&network_value);
    payload.insert(payload.end(), ptr, ptr + 4);
}

void Message::append16(int16_t value){
    uint16_t network_value = htons(value);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&network_value);
    payload.insert(payload.end(), ptr, ptr + 2);
}

void Message::append8(int8_t value){
    payload.push_back(static_cast<uint8_t>(value));
}

void Message::append_varint(uint32_t value){
    while (true) {
        uint8_t byte = value & 0x7F;
        value >>= 7;
        if (value != 0) {
            byte |= 0x80;
        }
        append8(byte);
        if (value == 0) {
            break;
        }
    }
}

void Message::append(const char* data, size_t size){
    payload.insert(payload.end(), data, data + size);
}

std::vector<uint8_t> Message::to_vec(){
    uint32_t size = htonl(payload.size());
    std::vector<uint8_t> result;
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&size);
    result.insert(result.end(), ptr, ptr + 4);
    result.insert(result.end(), payload.begin(), payload.end());
    return result;
}

std::vector<uint8_t> receive_message(int client_fd){
    uint32_t size;
    if(recv(client_fd, &size, sizeof(size), 0) <= 0){
        throw std::runtime_error("Failed to receive message");
    }
    size = ntohl(size);
    std::vector<uint8_t> buffer(size);
    recv(client_fd, buffer.data(), size, 0);
    return buffer;
}

Message parse_message(int client_fd){
    std::vector<uint8_t> raw_message = receive_message(client_fd);
    Message message;
    message.payload = raw_message;
    return message;
}

uint32_t read_varint(const Message& msg, size_t& offset) {
    uint32_t value = 0;
    int shift = 0;
    while (offset < msg.payload.size()) {
        uint8_t b = msg.payload[offset++];
        value |= (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            break;
        }
        shift += 7;
    }
    return value;
}
uint64_t read_varint(const std::vector<uint8_t>& buffer, size_t& offset){
    uint64_t value = 0;
    int shift = 0;
    while (offset < buffer.size()) {
        uint8_t b = buffer[offset++];
        value |= (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            break;
        }
        shift += 7;
    }
    return value;
}
uint64_t read_zigzag_varint(const std::vector<uint8_t>& buffer, size_t& offset){
    uint64_t value = 0;
    int shift = 0;
    while (offset < buffer.size()) {
        uint8_t b = buffer[offset++];
        value |= (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            break;
        }
        shift += 7;
    }
    return (value >> 1) ^ -(value & 1);
}
int32_t read_32_be(const std::vector<uint8_t>& buffer, size_t& offset) {
    if (offset + 4 > buffer.size()) {
        return 0;
    }
    uint32_t val = (static_cast<uint32_t>(buffer[offset]) << 24) | 
                   (static_cast<uint32_t>(buffer[offset + 1]) << 16) | 
                   (static_cast<uint32_t>(buffer[offset + 2]) << 8) | 
                   static_cast<uint32_t>(buffer[offset + 3]);
                   
    offset += 4;
    return static_cast<int32_t>(val);
}