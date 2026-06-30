#include "response.hpp"
#include <filesystem>

Message create_response(Message& request){
    size_t offset = 0;
    request_header_v2 req_header(request.payload, offset);
    Message response;
    int16_t api_key = req_header.api_key;
    int16_t api_version = req_header.api_version;
    if(api_key == 18){
        request_header_v0 resp_header(req_header.correlation_id);
        resp_header.append_to(response);
        if(req_header.api_version <= 4 and req_header.api_version >= 0){
            ApiVersionsResponseBody body;
            body.error_code = 0;
            body.throttle_time = 0;
            ApiVersionsResponseKey api_key_api_versions;
            api_key_api_versions.api_key = 18;
            api_key_api_versions.min_version = 0;
            api_key_api_versions.max_version = 4;
            body.api_keys.push_back(api_key_api_versions);
            ApiVersionsResponseKey api_key_describe_partitions;
            api_key_describe_partitions.api_key = 75;
            api_key_describe_partitions.min_version = 0;
            api_key_describe_partitions.max_version = 0;
            body.api_keys.push_back(api_key_describe_partitions);
            ApiVersionsResponseKey api_key_fetch;
            api_key_fetch.api_key = 1;
            api_key_fetch.min_version = 0;
            api_key_fetch.max_version = 16;
            body.api_keys.push_back(api_key_fetch);
            ApiVersionsResponseKey api_key_produce;
            api_key_produce.api_key = 0;
            api_key_produce.min_version = 0;
            api_key_produce.max_version = 12;
            body.api_keys.push_back(api_key_produce);
            serialize_apiversions_body(body, response);
        }
        else{
            response.append16(35);
        }
    }
    else if (req_header.api_key == 75) {
        response_header_v1 resp_header(req_header.correlation_id);
        resp_header.append_to(response);
        DescribeTopicPartitionsRequest request_body = deserialize_describe_topic_partitions_request(request, offset);
        DescribeTopicPartitionsResponse response_body;
        response_body.throttle_time_ms = 0;
        for (const auto& topic_req : request_body.topics) {
            TopicResponse t_resp;
            t_resp.name = topic_req.name;
            if (cluster_cache.topics.count(topic_req.name)) {
                TopicState state = cluster_cache.topics[topic_req.name];
                
                t_resp.error_code = 0;
                std::memcpy(t_resp.topic_id, state.topic_id.data(), 16);
                t_resp.is_internal = false;
                t_resp.topic_authorized_operations = 0;
                
                for (const auto& p : state.partitions) {
                    PartitionResponse p_resp;
                    p_resp.error_code = 0;
                    p_resp.partition_index = p.partition_id;
                    p_resp.leader_id = p.leader_id;
                    p_resp.leader_epoch = p.leader_epoch;
                    p_resp.replica_nodes = p.replicas;
                    p_resp.isr_nodes = p.isr;
                    
                    t_resp.partitions.push_back(p_resp);
                }
                std::sort(t_resp.partitions.begin(), t_resp.partitions.end(), 
                    [](const PartitionResponse& a, const PartitionResponse& b) {
                        return a.partition_index < b.partition_index;
                    });
            } else {
                t_resp.error_code = 3;
                std::memset(t_resp.topic_id, 0, 16); 
                t_resp.is_internal = false;
                t_resp.topic_authorized_operations = 0;
            }
            response_body.topics.push_back(t_resp);
        }
        std::sort(response_body.topics.begin(), response_body.topics.end(), 
            [](const TopicResponse& a, const TopicResponse& b) {
                return a.name < b.name;
            });
        serialize_describe_topic_partitions_response(response_body, response);
    }
    else if (api_key == 1) {
        response_header_v1 resp_header(req_header.correlation_id);
        resp_header.append_to(response);
        FetchRequest request_body = deserialize_fetch_request(request, offset);
        FetchResponse response_body;
        response_body.session_id = request_body.session_id;
        for (const auto& topic_req : request_body.topics) {
            FetchTopicResponse t_resp;
            std::memcpy(t_resp.topic_id, topic_req.topic_id, 16);
            std::string uuid(reinterpret_cast<const char*>(topic_req.topic_id), 16);
            std::string target_name = cluster_cache.uuid_to_name[uuid];
            if (target_name.empty()) {
                for (const auto& p_req : topic_req.partitions) {
                    FetchPartitionResponse p_resp;
                    p_resp.partition_index = p_req.partition;
                    p_resp.error_code = 100; 
                    t_resp.partitions.push_back(p_resp);
                }
            } else {
                for (const auto& p_req : topic_req.partitions) {
                    FetchPartitionResponse p_resp;
                    p_resp.partition_index = p_req.partition;
                    bool partition_exists = false;
                    for (const auto& part : cluster_cache.topics[target_name].partitions) {
                        if (part.partition_id == p_req.partition) {
                            partition_exists = true;
                            break;
                        }
                    }

                    if (!partition_exists) {
                        p_resp.error_code = 3;
                    } else {
                        p_resp.error_code = 0;
                        
                        std::string part_dir = server_log_dir + "/" + target_name + "-" + 
                                                std::to_string(p_req.partition);
                        std::vector<std::string> log_files;
                        if (std::filesystem::exists(part_dir) && std::filesystem::is_directory(part_dir)) {
                            for (const auto& entry : std::filesystem::directory_iterator(part_dir)) {
                                if (entry.path().extension() == ".log") {
                                    log_files.push_back(entry.path().string());
                                }
                            }
                            std::sort(log_files.begin(), log_files.end());
                        }
                        
                        for (const auto& log_file : log_files) {
                            std::ifstream file(log_file, std::ios::binary | std::ios::ate);
                            if (!file.is_open()) continue;
                            ssize_t file_size = file.tellg();
                            file.seekg(0, std::ios::beg);
                            if (file_size <= 0) continue;
                            
                            std::vector<uint8_t> file_buffer(file_size);
                            file.read(reinterpret_cast<char*>(file_buffer.data()), file_size);
                            size_t file_offset = 0;
                            while (file_offset < file_size) {
                                if (file_offset + 12 > file_size) break; 
                                uint64_t base_offset = 0;
                                for (int i = 0; i < 8; ++i) {
                                    base_offset = (base_offset << 8) | file_buffer[file_offset + i];
                                }
                                uint32_t batch_length = 0;
                                for (int i = 8; i < 12; ++i) {
                                    batch_length = (batch_length << 8) | file_buffer[file_offset + i];
                                }
                                size_t total_batch_bytes = 12 + batch_length;
                                if (file_offset + total_batch_bytes > file_size) break;
                                if (static_cast<int64_t>(base_offset) >= p_req.fetch_offset) {
                                    p_resp.records.insert(
                                        p_resp.records.end(),
                                        file_buffer.begin() + file_offset,
                                        file_buffer.begin() + file_offset + total_batch_bytes
                                    );
                                }
                                file_offset += total_batch_bytes;
                            }
                        }
                    }
                    t_resp.partitions.push_back(p_resp);
                }
            }
            response_body.topics.push_back(t_resp);
        }
        serialize_fetch_response(response_body, response);
    }
    else if (api_key == 0) {
        response_header_v1 resp_header(req_header.correlation_id);
        resp_header.append_to(response);
        ProduceRequest request_body = deserialize_produce_request(request, offset);
        ProduceResponse response_body;
        for (const auto& topic_req : request_body.topics) {
            ProduceTopicResponse t_resp;
            t_resp.name = topic_req.name;
            if (!cluster_cache.topic_exists(topic_req.name)) {
                for (const auto& p_req : topic_req.partitions) {
                    ProducePartitionResponse p_resp;
                    p_resp.partition_index = p_req.partition_index;
                    p_resp.error_code = 3;
                    t_resp.partitions.push_back(p_resp);
                }
            } else {
                TopicState& state = cluster_cache.topics[topic_req.name];
                for (const auto& p_req : topic_req.partitions) {
                    ProducePartitionResponse p_resp;
                    p_resp.partition_index = p_req.partition_index;
                    bool partition_exists = false;
                    for (const auto& part : state.partitions) {
                        if (part.partition_id == p_req.partition_index) {
                            partition_exists = true;
                            break;
                        }
                    }
                    if (!partition_exists) {
                        p_resp.error_code = 3;
                    } else {
                        p_resp.error_code = 0;
                        p_resp.base_offset = 0;
                        p_resp.log_append_time_ms = -1;
                        p_resp.log_start_offset = 0;

                        if (!p_req.records.empty()) {
                            std::string part_dir = server_log_dir + "/" + topic_req.name + "-" +
                                                    std::to_string(p_req.partition_index);
                            std::filesystem::create_directories(part_dir);
                            std::string log_path = part_dir + "/00000000000000000000.log";
                            std::ofstream out(log_path, std::ios::binary | std::ios::app);
                            if (out.is_open()) {
                                out.write(reinterpret_cast<const char*>(p_req.records.data()), p_req.records.size());
                            }
                        }
                    }
                    t_resp.partitions.push_back(p_resp);
                }
            }
            response_body.topics.push_back(t_resp);
        }
        serialize_produce_response(response_body, response);
    }
    
    return response;
}