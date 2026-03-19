#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <variant>
#include <optional>
#include <cstdint>

namespace labcompress {

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