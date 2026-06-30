#include "metadata.hpp"

ClusterMetadata cluster_cache;

void parse_metadata_file(const std::string& filepath){
    std::ifstream metadata_file(filepath, std::ios::binary);
    ssize_t size;
    if(metadata_file.is_open()){
        metadata_file.seekg(0, std::ios::end);
        size = metadata_file.tellg();
        metadata_file.seekg(0, std::ios::beg);
        if (size <= 0) return;
        std::vector<uint8_t> buffer;
        buffer.resize(size);
        metadata_file.read(reinterpret_cast<char*>(buffer.data()), size);
        size_t offset = 0;
        std::unordered_map<std::string, std::string> temp_uuid_to_name;
        while (offset < buffer.size()) {
            if (offset + 61 > buffer.size()) break;
            size_t batch_start = offset; 
            offset += 8;
            uint32_t batch_length = read_32_be(buffer, offset);
            size_t next_batch_offset = batch_start + 12 + batch_length; 
            offset += 45;
            int32_t records_count = read_32_be(buffer, offset);
            
            for (int i = 0; i < records_count; i++) {
                int64_t record_length = read_zigzag_varint(buffer, offset);
                size_t record_end = offset + record_length;

                offset++;
                read_zigzag_varint(buffer, offset);
                read_zigzag_varint(buffer, offset);
                
                int64_t key_length = read_zigzag_varint(buffer, offset);
                if (key_length > 0) {
                    offset += key_length; 
                }
                
                int64_t value_length = read_zigzag_varint(buffer, offset);
                if (value_length > 0) {
                    uint8_t frame_version = buffer[offset++]; 
                    uint8_t frame_type = buffer[offset++];
                    if (frame_type == 2) {
                        offset++; 
                        uint32_t name_len = static_cast<uint32_t>(read_varint(buffer, offset)) - 1;
                        std::string name(buffer.begin() + offset, buffer.begin() + offset + name_len);
                        offset += name_len;
                        
                        std::string uuid(buffer.begin() + offset, buffer.begin() + offset + 16);
                        offset += 16;
                        
                        temp_uuid_to_name[uuid] = name;
                        
                        TopicState new_topic;
                        new_topic.name = name;
                        std::memcpy(new_topic.topic_id.data(), uuid.data(), 16);
                        
                        cluster_cache.topics[name] = new_topic;
                        cluster_cache.uuid_to_name[uuid] = name;
                        std::cerr << "[METADATA] Found Topic: " << name << std::endl;
                        
                    } else if (frame_type == 3) {
                        offset++;
                        int32_t partition_id = read_32_be(buffer, offset);
                        
                        std::string uuid(buffer.begin() + offset, buffer.begin() + offset + 16);
                        offset += 16;
                        
                        PartitionState new_partition;
                        new_partition.partition_id = partition_id;
                    
                        uint32_t replica_count = static_cast<uint32_t>(read_varint(buffer, offset)) - 1;
                        for (uint32_t j = 0; j < replica_count; ++j) {
                            new_partition.replicas.push_back(read_32_be(buffer, offset));
                        }
                    
                        uint32_t isr_count = static_cast<uint32_t>(read_varint(buffer, offset)) - 1;
                        for (uint32_t j = 0; j < isr_count; ++j) {
                            new_partition.isr.push_back(read_32_be(buffer, offset));
                        }
                    
                        uint32_t rm_replica_count = static_cast<uint32_t>(read_varint(buffer, offset)) - 1;
                        offset += rm_replica_count * 4; 
                        
                        uint32_t add_replica_count = static_cast<uint32_t>(read_varint(buffer, offset)) - 1;
                        offset += add_replica_count * 4; 
                        
                        new_partition.leader_id = read_32_be(buffer, offset);
                        new_partition.leader_epoch = read_32_be(buffer, offset);
                        
                        if (temp_uuid_to_name.count(uuid)) {
                            std::string target_topic_name = temp_uuid_to_name[uuid];
                            cluster_cache.topics[target_topic_name].partitions.push_back(new_partition);
                            
                            std::cerr << "[METADATA] Mapped Partition " << partition_id << " to " << target_topic_name << std::endl;
                        }
                    }
                }
                
                offset = record_end;
            }
            offset = next_batch_offset;
        }
    } else {
        std::cerr << "[METADATA PARSER] ERROR: Could not open file at " << filepath << std::endl;
    }
}