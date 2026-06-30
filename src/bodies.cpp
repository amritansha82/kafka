#include "bodies.hpp"

void serialize_api_key(const ApiVersionsResponseKey& key, Message& msg){
    msg.append16(key.api_key);
    msg.append16(key.min_version);
    msg.append16(key.max_version);
    msg.append8(0);
}
void serialize_apiversions_body(const ApiVersionsResponseBody& body, Message& msg){
    msg.append16(body.error_code);
    msg.append_varint((uint32_t)(body.api_keys.size() + 1));
    for(auto& key : body.api_keys){
        serialize_api_key(key, msg);
    }
    msg.append32(body.throttle_time);
    msg.append8(0);
}

DescribeTopicPartitionsRequest deserialize_describe_topic_partitions_request(const Message& request, size_t& offset) {
    DescribeTopicPartitionsRequest req;
    uint32_t array_len = read_varint(request, offset); 
    size_t num_topics = array_len - 1; 
    for (size_t i = 0; i < num_topics; i++) {
        uint32_t str_len = read_varint(request, offset);
        size_t actual_str_len = str_len - 1;
        std::string topic_name(
            request.payload.begin() + offset, 
            request.payload.begin() + offset + actual_str_len
        );
        offset += actual_str_len;
        uint32_t topic_tags = read_varint(request, offset); 
        req.topics.push_back({topic_name});
    }
    uint32_t request_tags = read_varint(request, offset);
    return req;
}

void serialize_describe_topic_partitions_response(const DescribeTopicPartitionsResponse& body, Message& response) {
    response.append32(body.throttle_time_ms);
    response.append_varint(body.topics.size() + 1);
    for (const auto& topic : body.topics) {
        response.append16(topic.error_code);
        response.append_varint(topic.name.length() + 1);
        response.append(topic.name.c_str(), topic.name.length());
        response.append(reinterpret_cast<const char*>(topic.topic_id), 16);
        response.append8(topic.is_internal ? 1 : 0);
        response.append_varint(topic.partitions.size() + 1);
        for (const auto& p : topic.partitions) {
            response.append16(p.error_code);
            response.append32(p.partition_index);
            response.append32(p.leader_id);
            response.append32(p.leader_epoch);
            response.append_varint(p.replica_nodes.size() + 1);
            for (int32_t node : p.replica_nodes) {
                response.append32(node);
            }
            response.append_varint(p.isr_nodes.size() + 1);
            for (int32_t node : p.isr_nodes) {
                response.append32(node);
            }
            response.append_varint(1);
            response.append_varint(1);
            response.append_varint(1);
            response.append8(0);
        }   
        response.append32(topic.topic_authorized_operations);
        response.append8(0);
    }
    response.append8(body.next_cursor);
    response.append8(0); 
}

FetchRequest deserialize_fetch_request(const Message& request, size_t& offset) {
    FetchRequest req;

    req.max_wait_ms = read_32_be(request.payload, offset);
    req.min_bytes = read_32_be(request.payload, offset);
    req.max_bytes = read_32_be(request.payload, offset);
    req.isolation_level = request.payload[offset++];
    req.session_id = read_32_be(request.payload, offset);
    req.session_epoch = read_32_be(request.payload, offset);

    uint32_t topics_count = read_varint(request, offset) - 1;

    for (uint32_t i = 0; i < topics_count; i++) {
        FetchTopicRequest topic_req;
        std::memcpy(topic_req.topic_id, &request.payload[offset], 16);
        offset += 16;
        uint32_t parts_count = read_varint(request, offset) - 1;
        for (uint32_t j = 0; j < parts_count; j++) {
            FetchPartitionRequest p_req;
            p_req.partition = read_32_be(request.payload, offset);
            p_req.current_leader_epoch = read_32_be(request.payload, offset);
            uint64_t offset_val = 0;
            for(int k = 0; k < 8; k++) { offset_val = (offset_val << 8) | request.payload[offset++]; }
            p_req.fetch_offset = static_cast<int64_t>(offset_val);
            p_req.last_fetched_epoch = read_32_be(request.payload, offset);
            uint64_t log_val = 0;
            for(int k = 0; k < 8; k++) { log_val = (log_val << 8) | request.payload[offset++]; }
            p_req.log_start_offset = static_cast<int64_t>(log_val);
            p_req.partition_max_bytes = read_32_be(request.payload, offset);
            offset++;
            topic_req.partitions.push_back(p_req);
        }
        offset++;
        req.topics.push_back(topic_req);
    }
    return req;
}

void serialize_fetch_response(const FetchResponse& body, Message& response) {
    response.append32(body.throttle_time_ms);
    response.append16(body.error_code);
    response.append32(body.session_id);
    response.append_varint(body.topics.size() + 1);
    for (const auto& topic : body.topics) {
        response.append(reinterpret_cast<const char*>(topic.topic_id), 16);
        response.append_varint(topic.partitions.size() + 1);
        for (const auto& p : topic.partitions) {
            response.append32(p.partition_index);
            response.append16(p.error_code);
            response.append64(p.high_watermark);
            response.append64(p.last_stable_offset);
            response.append64(p.log_start_offset);
            response.append_varint(1);
            response.append32(0);
            if (p.records.empty()) {
                response.append_varint(0);
            } else {
                response.append_varint(p.records.size() + 1);
                response.append(reinterpret_cast<const char*>(p.records.data()), p.records.size());
            }
            response.append8(0);
        }
        response.append8(0);
    }
    response.append8(0);
}

ProduceRequest deserialize_produce_request(const Message& request, size_t& offset) {
    ProduceRequest req;
    uint32_t txn_id_len = read_varint(request, offset);
    if (txn_id_len > 1) {
        req.transactional_id = std::string(
            request.payload.begin() + offset,
            request.payload.begin() + offset + (txn_id_len - 1)
        );
        offset += (txn_id_len - 1);
    }

    uint16_t acks_raw;
    std::memcpy(&acks_raw, &request.payload[offset], 2);
    req.acks = static_cast<int16_t>(ntohs(acks_raw));
    offset += 2;

    req.timeout_ms = read_32_be(request.payload, offset);

    uint32_t topics_count = read_varint(request, offset) - 1;
    for (uint32_t i = 0; i < topics_count; i++) {
        ProduceTopicRequest topic_req;

        uint32_t name_len = read_varint(request, offset) - 1;
        topic_req.name = std::string(
            request.payload.begin() + offset,
            request.payload.begin() + offset + name_len
        );
        offset += name_len;

        uint32_t parts_count = read_varint(request, offset) - 1;
        for (uint32_t j = 0; j < parts_count; j++) {
            ProducePartitionRequest p_req;

            p_req.partition_index = read_32_be(request.payload, offset);

            uint32_t records_len = read_varint(request, offset) - 1;
            if (records_len > 0 && records_len != 0xFFFFFFFF) {
                p_req.records.assign(
                    request.payload.begin() + offset,
                    request.payload.begin() + offset + records_len
                );
                offset += records_len;
            }

            read_varint(request, offset);

            topic_req.partitions.push_back(p_req);
        }

        read_varint(request, offset);

        req.topics.push_back(topic_req);
    }

    read_varint(request, offset);

    return req;
}

void serialize_produce_response(const ProduceResponse& body, Message& response) {
    response.append_varint(body.topics.size() + 1);
    for (const auto& topic : body.topics) {
        response.append_varint(topic.name.length() + 1);
        response.append(topic.name.c_str(), topic.name.length());
        response.append_varint(topic.partitions.size() + 1);
        for (const auto& p : topic.partitions) {
            response.append32(p.partition_index);
            response.append16(p.error_code);
            response.append64(p.base_offset);
            response.append64(p.log_append_time_ms);
            response.append64(p.log_start_offset);
            response.append_varint(1);
            response.append8(0);
            response.append8(0);
        }
        response.append8(0);
    }
    response.append32(body.throttle_time_ms);
    response.append8(0);
}