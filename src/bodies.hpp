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
#include "codec.hpp"

struct ApiVersionsResponseKey {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

struct ApiVersionsResponseBody {
    int16_t error_code;
    int32_t throttle_time;
    std::vector<ApiVersionsResponseKey> api_keys;
};

void serialize_api_key(ApiVersionsResponseKey& key, Message& msg);
void serialize_apiversions_body(const ApiVersionsResponseBody& body, Message& msg);

struct TopicRequest {
    std::string name;
};

struct DescribeTopicPartitionsRequest {
    std::vector<TopicRequest> topics;
};

struct PartitionResponse {
    int16_t error_code;
    int32_t partition_index;
    int32_t leader_id;
    int32_t leader_epoch;
    std::vector<int32_t> replica_nodes;
    std::vector<int32_t> isr_nodes;
    std::vector<int32_t> eligible_leader_replicas;
    std::vector<int32_t> last_known_elr;
    std::vector<int32_t> offline_replicas;
};

struct TopicResponse {
    int16_t error_code;
    std::string name;
    uint8_t topic_id[16];
    bool is_internal;
    std::vector<PartitionResponse> partitions;
    int32_t topic_authorized_operations;
};

struct DescribeTopicPartitionsResponse {
    int32_t throttle_time_ms = 0;
    std::vector<TopicResponse> topics;
    int8_t next_cursor = -1;
};

struct FetchPartitionRequest {
    int32_t partition;
    int32_t current_leader_epoch;
    int64_t fetch_offset;
    int32_t last_fetched_epoch;
    int64_t log_start_offset;
    int32_t partition_max_bytes;
};

struct FetchTopicRequest {
    uint8_t topic_id[16];
    std::vector<FetchPartitionRequest> partitions;
};

struct FetchRequest {
    int32_t max_wait_ms;
    int32_t min_bytes;
    int32_t max_bytes;
    int8_t isolation_level;
    int32_t session_id;
    int32_t session_epoch;
    std::vector<FetchTopicRequest> topics;
};

struct FetchPartitionResponse {
    int32_t partition_index;
    int16_t error_code = 0;
    int64_t high_watermark = 0;
    int64_t last_stable_offset = 0;
    int64_t log_start_offset = 0;
    std::vector<uint8_t> records;
};

struct FetchTopicResponse {
    uint8_t topic_id[16];
    std::vector<FetchPartitionResponse> partitions;
};

struct FetchResponse {
    int32_t throttle_time_ms = 0;
    int16_t error_code = 0;
    int32_t session_id = 0;
    std::vector<FetchTopicResponse> topics;
};

FetchRequest deserialize_fetch_request(const Message& request, size_t& offset);
void serialize_fetch_response(const FetchResponse& body, Message& response);
DescribeTopicPartitionsRequest deserialize_describe_topic_partitions_request(const Message& request, size_t& offset);
void serialize_describe_topic_partitions_response(const DescribeTopicPartitionsResponse& body, Message& response);

struct ProducePartitionRequest {
    int32_t partition_index;
    std::vector<uint8_t> records;
};

struct ProduceTopicRequest {
    std::string name;
    std::vector<ProducePartitionRequest> partitions;
};

struct ProduceRequest {
    std::string transactional_id;
    int16_t acks;
    int32_t timeout_ms;
    std::vector<ProduceTopicRequest> topics;
};

struct ProducePartitionResponse {
    int32_t partition_index;
    int16_t error_code = 0;
    int64_t base_offset = -1;
    int64_t log_append_time_ms = -1;
    int64_t log_start_offset = -1;
};

struct ProduceTopicResponse {
    std::string name;
    std::vector<ProducePartitionResponse> partitions;
};

struct ProduceResponse {
    int32_t throttle_time_ms = 0;
    std::vector<ProduceTopicResponse> topics;
};

ProduceRequest deserialize_produce_request(const Message& request, size_t& offset);
void serialize_produce_response(const ProduceResponse& body, Message& response);