#pragma once 
#include "schemaparser.hpp"
#include <vector>
#include <cstdint> 
#include <fstream> 

namespace labcompress {

    // CompressionEngine handles data compression and decompression
    constexpr uint32_t MAGIC_NUMBER = 0x5554494C; // 'UTIL' in hex 
    constexpr uint32_t FORMAT_VERSION = 1;

    struct BlockHeader {
        uint32_t year; 
        uint32_t month; 
        uint32_t experiment_count; 
        uint32_t compressed_size;
        uint64_t offset; 
    };

    class Compressor {
        public: 
            Compressor(const DataSchema& schema); 

            // Compress input CSV file to binary format
            void compress(const std::string& input_csv, const std::string& output_bin);
        private:
            DataSchema schema_;

            // Dictionary storage 
            std::unordered_map<size_t, std::unordered_map<std::string, uint16_t>> dictionaries_;

            void build_dictionaries(const std::string& input_csv); 
            void write_header(std::ofstream& out);
            void write_dictionaries(std::ofstream& out);
            void write_monthly_blocks(const std::string& input_csv, std::ofstream& out);

            // Encoding helpers
            void encode_variant(std::ofstream& out, uint64_t value);
            void encode_string_dict(std::ofstream& out, size_t col_index, const std::string& value);
            void encode_timestamp_delta(std::ofstream& out, const std::string& timestamp, uint64_t& prev_timestamp);
    };

} // namespace labcompress