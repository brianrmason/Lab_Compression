#include "schemaparser.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <regex>
#include <set>
#include <iostream>

// @version 1.0 12/17/2025
// Schema parsing and type detection implemented 
// Supports CSV files with header row
// Identifies compression strategies based on data characteristics 

namespace labcompress {

    bool read_csv_record(std::ifstream& file, std::string& record) {
        record.clear();
        std::string line;
        bool in_quotes = false;

        while (std::getline(file, line)) {
            if (!record.empty()) {
                record += '\n'; // preserve newline within quoted field
            }
            record += line;

            // Count quotes to determine if we're still inside a quoted field
            for (char c : line) {
                if (c == '"') in_quotes = !in_quotes;
            }

            if (!in_quotes) {
                return true; // complete record
            }
        }

        // EOF reached — return whatever we have
        return !record.empty();
    }

    std::vector<std::string> parse_csv_line(const std::string& line) {
        std::vector<std::string> fields; 
        std::string current; 
        bool in_quotes = false;

        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i]; 

            if(c == '"') {
                in_quotes = !in_quotes; 
            } else if (c == ',' && !in_quotes) {
                size_t start = current.find_first_not_of(" \t\r\n");
                size_t end = current.find_last_not_of(" \t\r\n");
                if (start != std::string::npos) {
                    current = current.substr(start, end - start + 1);
                }
                fields.push_back(current);
                current.clear();
            } else {
                current += c;
            }
        }

        size_t start = current.find_first_not_of(" \t\r\n");
        size_t end = current.find_last_not_of(" \t\r\n");
        if (start !=std::string::npos) {
            current = current.substr(start, end - start + 1);
        }
        fields.push_back(current);

        return fields;
    }

    ColumnType infer_type(const std::vector<std::string>& samples) {
        // Trim whitespace from samples
        std::vector<std::string> trimmed_samples;
        for (const auto& s : samples) {
            std::string trimmed = s;
            trimmed.erase(0, trimmed.find_first_not_of(" \t\r\n"));
            trimmed.erase(trimmed.find_last_not_of(" \t\r\n") + 1);
            trimmed_samples.push_back(trimmed);
        }

        if (samples.empty()) return ColumnType::STRING;

        // Check for GUID/UUID pattern - keep as STRING (we need these for record identification)
        std::regex guid_regex(R"([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})", 
                            std::regex::icase);
        if (std::regex_search(samples[0], guid_regex)) {
            return ColumnType::STRING;
        }
        
        // Check for date/time patterns (ISO: YYYY-MM-DD HH:MM:SS and US: M/D/YYYY H:MM)
        std::regex datetime_regex(R"(\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}|\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2})");
        if (std::regex_search(samples[0], datetime_regex)) {
            return ColumnType::TIMESTAMP;
        }
        
        // Check if all are numeric - BE MORE STRICT
        bool all_int = true;
        bool all_float = true;
        int non_empty = 0;
        
        for (const auto& s : samples) {
            if (s.empty()) continue;
            non_empty++;
            
            // Check integer
            try {
                size_t pos;
                std::stoll(s, &pos);
                if (pos != s.length()) all_int = false;
            } catch (...) {
                all_int = false;
            }
            
            // Check float
            if (all_int) {
                all_float = false;  // If it's an int, it's not a float
            } else {
                try {
                    size_t pos;
                    std::stod(s, &pos);
                    if (pos != s.length()) all_float = false;
                } catch (...) {
                    all_float = false;
                }
            }
            
            // Early exit if we know it's not numeric
            if (!all_int && !all_float) break;
        }
        
        if (all_int && non_empty > 0) return ColumnType::INTEGER;
        if (all_float && non_empty > 0) return ColumnType::FLOAT;
        
        // Check if categorical
        std::set<std::string> unique(samples.begin(), samples.end());
        
        if ((unique.size() < samples.size() * 0.5) && (unique.size() < 500)) {
            return ColumnType::CATEGORICAL;
        }

        return ColumnType::STRING;
    }

    DataSchema DataSchema::from_csv(const std::string& filepath, size_t sample_rows) {
        DataSchema schema; 
        std::ifstream file(filepath);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + filepath);
        }

        size_t last_slash = filepath.find_last_of("/\\");
        std::string filename = (last_slash != std::string::npos) 
            ? filepath.substr(last_slash + 1) 
            : filepath;

        size_t underscore = filename.find("_");
        if (underscore != std::string::npos) {
            schema.instrument_id = filename.substr(0, underscore); 
        }

        // Read header line
        std::string header_line;
        std::getline(file, header_line);
        auto headers = parse_csv_line(header_line);

        // Initialize column 
        for (size_t i = 0; i < headers.size(); ++i) {
            ColumnMetadata col; 
            col.name = headers[i];
            col.index = i;
            schema.columns.push_back(col);
            schema.column_index[col.name] = i; 
        }

        // Sample data for type inference
        std::vector<std::vector<std::string>> samples(headers.size());
        std::string line;
        size_t row = 0;

        while (read_csv_record(file, line) && row < sample_rows) {
            auto fields = parse_csv_line(line);
            for (size_t i = 0; i < fields.size() && i < headers.size(); ++i) {
                samples[i].push_back(fields[i]);
            }
            row++;
        }

        schema.row_count = row;

        // Infer column types and gather statistics
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            auto& col = schema.columns[i]; 
            col.type = infer_type(samples[i]);

            // Gather stats 
            std::set<std::string> unique(samples[i].begin(), samples[i].end()); 
            col.unique_count = unique.size();

            if (col.type == ColumnType::INTEGER || col.type == ColumnType::FLOAT) {
                std::vector<double> values; 
                for (const auto& s: samples[i]) {
                    if (!s.empty()) {
                        try {
                            values.push_back(std::stod(s)); 
                        } catch (...) {
                            // Ignore conversion errors
                        }
                    }
                }

                if (!values.empty()) {
                    col.min_value = *std::min_element(values.begin(), values.end());
                    col.max_value = *std::max_element(values.begin(), values.end());

                    col.is_monotonic = std::is_sorted(values.begin(), values.end()); 
                }
            }
            
        }

        file.close();
        return schema;

    }

    void DataSchema::analyze_compression_opportunities() {
        std::cout << "\n=== Analyzing compression opportunities ===\n" << schema_name << "\n";
        std::cout << "File: " << instrument_id << "\n";
        std::cout << "Rows Sampled: " << row_count << "\n";
        std::cout << "Columns: " << columns.size() << "\n\n";

        size_t total_categorical = 0;
        size_t total_timestamps = 0;
        size_t total_constants = 0;

        for (const auto& col: columns) {
            std::cout << "Column: " << col.name << "\n"; 
            std::cout << " Type: "; 

            switch (col.type) {
                case ColumnType::TIMESTAMP:
                    std::cout << "TIMESTAMP\n"; 
                    if (col.is_monotonic) {
                        std::cout << "  - Monotonic timestamps use delta encoding.\n";
                    }
                    total_timestamps++;
                    break;

                case ColumnType::INTEGER:
                    std::cout << "INTEGER\n"; 
                    if (col.min_value && col.max_value) {
                        std::cout << "  - Range: " << *col.min_value << " to " << *col.max_value << "\n";

                        // Detect constant columns
                        if (*col.min_value == *col.max_value) {
                            std::cout << " Constant column detected. Consider run-length encoding.\n";
                            total_constants++;
                        } else if (*col.min_value == 0 && *col.max_value <= 1) {
                            std::cout << " Boolean-like column detected. Consider bit-packing.\n";
                        } else if (*col.max_value - *col.min_value < 10) {
                            std::cout << " Small range integer detected. Consider variant encoding.\n";
                        }
                    }
                    break;

                case ColumnType::FLOAT:
                    std::cout << "FLOAT\n"; 
                    break;

                case ColumnType::STRING:
                    std::cout << "STRING\n"; 
                    if (col.unique_count) {
                        std::cout << " - Unique values: " << *col.unique_count << "\n";
                        if (*col.unique_count < row_count * 0.5) {
                            std::cout << " - Consider dictionary encoding.\n";
                        }
                    }
                    break;

                case ColumnType::CATEGORICAL:
                    std::cout << "CATEGORICAL\n";
                    if (col.unique_count) {
                        std::cout << " - Unique values: " << *col.unique_count << "\n";
                        std::cout << " - Dictionary Encoding\n";
                        total_categorical++;
                    }
                    break;

                case ColumnType::BOOLEAN:
                    std::cout << "BOOLEAN\n"; 
                    break;

                default:
                    std::cout << "UNKNOWN\n";
            }

            std:: cout << "\n";
        }

        // Summary
        std::cout << "\n=== Compression Strategy Summary ===\n";
        std::cout << "Categorical columns: " << total_categorical << " (dictionary encoding)\n";
        std::cout << "Timestamp columns: " << total_timestamps << " (delta encoding)\n";
        std::cout << "Constant columns: " << total_constants << " (run-length encoding)\n";
        std::cout << "\nExpected compression ratio: ~80-85%\n";

    }
}