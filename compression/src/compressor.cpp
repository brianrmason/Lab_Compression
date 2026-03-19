#include "../include/compressor.hpp"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <chrono> 
#include <iomanip>
#include <unordered_set>
#include <fstream>

namespace labcompress {

    // Parse CSV line 
    std::vector<std::string> parse_line(const std::string& line) {
        std::vector<std::string> fields; 
        std::stringstream ss(line); 
        std::string field; 
        
        while (std::getline(ss, field, ',')) {
            fields.push_back(field);
        }
        return fields;
    }
    
    // Convert timestamp string to epoch seconds
    uint64_t parse_timestamp(const std::string& ts) {
        std::tm tm = {};
        std::istringstream ss(ts);
        ss >> std::get_time(&tm, "%m/%d/%Y %H:%M");

        if (ss.fail()) {
            return 0; 
        }

        auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
        auto epoch = tp.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
    }

    Compressor::Compressor(const DataSchema& schema) : schema_(schema) {
        // Constructor implementation
    }

    // Build dictionaries for categorical/string columns
    void Compressor::build_dictionaries(const std::string& input_csv) {
        std::ifstream file(input_csv); 
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open input CSV file: " + input_csv);
        }

        // Skip header 
        std::string line;
        std::getline(file, line);

        // Collect unique values for dictionary building
        std::vector<std::unordered_set<std::string>> unique_values(schema_.columns.size());

        while (std::getline(file, line)) {
            auto fields = parse_line(line); 

            for (size_t i = 0; i < fields.size() && i < schema_.columns.size(); ++i) {
                if (schema_.columns[i].type == ColumnType::CATEGORICAL) {
                    unique_values[i].insert(fields[i]);
                }
            }
        }

        // Build dictionaries
        for (size_t i = 0; i < schema_.columns.size(); ++i) {
            if (schema_.columns[i].type == ColumnType::CATEGORICAL) {
                // Skip if dictionary would be too large
                if (unique_values[i].size() > 500) {
                    std::cout << "Skipping dictionary for " << schema_.columns[i].name 
                              << ": too many unique values (" << unique_values[i].size() << ")\n";
                    continue;
                }
                uint16_t id = 0; 
                for (const auto& value : unique_values[i]) {
                    dictionaries_[i][value] = id++;
                }

                std::cout << "Dictionary for " << schema_.columns[i].name 
                        << ": " << dictionaries_[i].size() << " entries\n";
            }
        }
    }

    void Compressor::encode_variant(std::ofstream& out, uint64_t value) {
        // Simple variable-length encoding
        while (value >= 0x80) {
            uint8_t byte = (value & 0x7F) | 0x80;
            out.put(static_cast<char>(byte));
            value >>= 7;
        }
        out.put(static_cast<char>(value & 0x7F));
    }

    void Compressor::encode_string_dict(std::ofstream& out, size_t col_index, const std::string& value) {
        auto it = dictionaries_[col_index].find(value); 
        if (it != dictionaries_[col_index].end()) {
            encode_variant(out, it->second);
        } else {
            encode_variant(out, 0); // Unknown value
        }
    }

    void Compressor::encode_timestamp_delta(std::ofstream& out, const std::string& timestamp, 
                                            uint64_t& prev_timestamp) {
        uint64_t ts = parse_timestamp(timestamp);
        if (prev_timestamp == 0) {
            out.write(reinterpret_cast<const char*>(&ts), sizeof(ts)); 
            prev_timestamp = ts; 
        } else {
            // Write delta 
            int64_t delta = static_cast<int64_t>(ts) - static_cast<int64_t>(prev_timestamp);
            encode_variant(out, static_cast<uint64_t>(delta));
            prev_timestamp = ts;
        }
    }
    
    void Compressor::write_header(std::ofstream& out) {
        // Magic number
        out.write(reinterpret_cast<const char*>(&MAGIC_NUMBER), sizeof(MAGIC_NUMBER));

        // Version 
        uint32_t version = FORMAT_VERSION;
        out.write(reinterpret_cast<const char*>(&version), sizeof(version));

        // Row count 
        uint32_t rows = static_cast<uint32_t>(schema_.row_count);
        out.write(reinterpret_cast<const char*>(&rows), sizeof(rows));

        // Column count
        uint32_t cols = static_cast<uint32_t>(schema_.columns.size());
        out.write(reinterpret_cast<const char*>(&cols), sizeof(cols));

        std::cout << "Header written: " << rows << " rows, " << cols << " columns\n";
    }

    void Compressor::write_dictionaries(std::ofstream& out) {
        // Write dictionary count 
        uint32_t dict_count = static_cast<uint32_t>(dictionaries_.size());
        out.write(reinterpret_cast<const char*>(&dict_count), sizeof(dict_count));

        // Write each dictionary 
        for (const auto& [col_index, dict] : dictionaries_) {
            // Column index 
            uint32_t idx = static_cast<uint32_t>(col_index);
            out.write(reinterpret_cast<const char*>(&idx), sizeof(idx));

            // Dictionary size
            uint32_t dict_size = static_cast<uint32_t>(dict.size());
            out.write(reinterpret_cast<const char*>(&dict_size), sizeof(dict_size));

            // Write entries
            for (const auto& [value, id] : dict) {
                // ID 
                out.write(reinterpret_cast<const char*>(&id), sizeof(id));

                // Value length 
                uint16_t len = static_cast<uint16_t>(value.length());
                out.write(reinterpret_cast<const char*>(&len), sizeof(len));

                // Value
                out.write(value.c_str(), len); 
            }
        }

        std::cout << "Dictionaries written: " << dict_count << " dictionaries\n";
    }

    void Compressor::compress(const std::string& input_csv, const std::string& output_bin) {
        std::cout << "\n=== Starting Compression ===\n"; 

        // Build dictionaries
        std::cout << "Building dictionaries...\n"; 
        build_dictionaries(input_csv);

        // Write output files 
        std::ofstream out(output_bin, std::ios::binary); 
        if (!out.is_open()) {
            throw std::runtime_error("Cannot create output file: " + output_bin);
        }

        // Write header 
        std::cout << "\n";
        write_header(out); 

        // Write dictionaries 
        write_dictionaries(out);

        // Compress data 
        std::ifstream in(input_csv);
        std::string line;
        std::getline(in, line); // Skip header

        uint32_t row_count = 0;

        // Track previous timestamp for delta encoding
        std::vector<uint64_t> prev_timestamps(schema_.columns.size(), 0); 

        while (std::getline(in, line)) {
            auto fields = parse_line(line); 

            // Encode each field based on type 
            for (size_t i = 0; i < schema_.columns.size(); ++i) {
                const auto& col = schema_.columns[i];
                std::string value = (i < fields.size()) ? fields[i] : "";

                if (col.type == ColumnType::CATEGORICAL) {
                    // Use dictionary encoding
                    encode_string_dict(out, i, value); 
                } else if (col.type == ColumnType::TIMESTAMP) {
                    // Delta encode timestamp
                    encode_timestamp_delta(out, value, prev_timestamps[i]);
                } else if (col.type == ColumnType::INTEGER) {
                    // Variant encode integer
                    if (!value.empty()) {
                        try {
                            int64_t val = std::stoll(value);
                            encode_variant(out, static_cast<uint64_t>(val));
                        } catch (...) {
                            encode_variant(out, 0); // Default for parse error
                        }
                    } else {
                        encode_variant(out, 0); // Default for null
                    }
                }
                else {
                    // STRING - write length + data
                    uint16_t len = static_cast<uint16_t>(value.length());
                    out.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    if (len > 0) {
                        out.write(value.c_str(), len);
                    }
                }
            }

            row_count++;
            if (row_count % 1000 == 0) {
                std::cout << "\rCompressed " << row_count << " rows..." << std::flush; 
            }
        }

        std::cout << "\n\nCompression complete!\n";
        std::cout << "Rows Compressed: " << row_count << "\n";
        std::cout << "Output: " << output_bin << "\n";

        out.close(); 
        in.close(); 

        std::ifstream in_size(input_csv, std::ios::ate); 
        std::ifstream out_size(output_bin, std::ios::ate); 

        size_t input_size = in_size.tellg();
        size_t output_size = out_size.tellg();

        double ratio = (1.0 - static_cast<double>(output_size) / input_size) * 100.0; 

        std::cout << "Input size: " << input_size << " bytes\n";
        std::cout << "Output size: " << output_size << " bytes\n";
        std::cout << "Compression ratio: " << std::fixed << std::setprecision(1) << ratio << "%\n\n";
    }


}