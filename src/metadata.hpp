#pragma once
#include <vector>
#include <string>
#include <array>
#include <cstdint>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include "codec.hpp"

struct PartitionState {
    int32_t partition_id;
    int32_t leader_id;
    int32_t leader_epoch;
    std::vector<int32_t> replicas;
    std::vector<int32_t> isr;
};

struct TopicState {
    std::string name;
    std::array<uint8_t, 16> topic_id;
    std::vector<PartitionState> partitions;
};

class ClusterMetadata {
public:
    std::unordered_map<std::string, TopicState> topics;
    std::unordered_map<std::string, std::string> uuid_to_name;
    bool topic_exists(const std::string& name) {
        return topics.find(name) != topics.end();
    }
};

extern ClusterMetadata cluster_cache;

void parse_metadata_file(const std::string& filepath);

