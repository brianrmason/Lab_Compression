#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <variant>
#include <optional>
#include <cstdint>
#include <fstream>

namespace labcompress {

    // Binary format constants (shared between compressor and query engine)
    constexpr uint32_t MAGIC_NUMBER = 0x5554494C; // 'UTIL' in hex
    constexpr uint32_t FORMAT_VERSION = 2;

    // CSV parsing utility (shared across modules)
    std::vector<std::string> parse_csv_line(const std::string& line);

    // Read a complete CSV record (handles multi-line quoted fields)
    bool read_csv_record(std::ifstream& file, std::string& record);

    enum class ColumnType {
        TIMESTAMP, 
        INTEGER, 
        FLOAT,
        STRING, 
        CATEGORICAL, 
        BOOLEAN
    };

    struct ColumnMetadata {
        std::string name; 
        ColumnType type; 
        size_t index; 

        std::optional<double> min_value; 
        std::optional<double> max_value; 
        std::optional<size_t> unique_count; 
        std::optional<size_t> null_count; 
        bool is_monotonic = false; 
        bool has_pattern = false; 

        std::unordered_map<std::string, uint16_t> dictionary;
        std::vector<std::string> reverse_dictionary;
    };

    struct DataSchema {
        std::string instrument_id; 
        std::string schema_name; 
        std::vector<ColumnMetadata> columns;
        std::unordered_map<std::string, size_t> column_index;

        size_t row_count = 0;
        size_t estimated_raw_size = 0;

        // Parse a CSV file to infer the data schema
        static DataSchema from_csv(const std::string& filepath, size_t sample_rows = 1000);

        void analyze_compression_opportunities();

        std::vector<std::byte> serialize() const;
        static DataSchema deserialize(const std::vector<std::byte>& data);
    };

}