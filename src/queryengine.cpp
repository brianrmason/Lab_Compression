#include "../include/queryengine.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <set>
#include <ctime>

namespace labcompress {

    uint64_t QueryEngine::decode_variant(std::ifstream& in) {
        uint64_t result = 0;
        int shift = 0;

        while (true) {
            char byte;
            if (!in.read(&byte, 1)) break;
            result |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) break;
            shift += 7;
            if (shift >= 64) break; // guard against corrupt data
        }
        return result;
    }

    std::string decode_timestamp(uint64_t epoch_seconds) {
        std::time_t t = static_cast<std::time_t>(epoch_seconds);
        std::tm* tm = std::localtime(&t);
        if (!tm) return "";

        std::ostringstream oss;
        oss << (tm->tm_year + 1900) << "-"
            << std::setfill('0') << std::setw(2) << (tm->tm_mon + 1) << "-"
            << std::setfill('0') << std::setw(2) << tm->tm_mday << " "
            << std::setfill('0') << std::setw(2) << tm->tm_hour << ":"
            << std::setfill('0') << std::setw(2) << tm->tm_min << ":"
            << std::setfill('0') << std::setw(2) << tm->tm_sec;
        return oss.str();
    }

    int extract_year(uint64_t epoch_seconds) {
        std::time_t t = static_cast<std::time_t>(epoch_seconds);
        std::tm* tm = std::localtime(&t);
        if (!tm) return 0;
        return tm->tm_year + 1900;
    }

    QueryEngine::QueryEngine(const std::string& binary_file)
        : binary_file_(binary_file) {
        load_data();
    }

    void QueryEngine::load_data() {
        std::ifstream in(binary_file_, std::ios::binary);

        if (!in.is_open()) {
            throw std::runtime_error("Cannot open binary file: " + binary_file_);
        }

        // Read header
        uint32_t magic;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (magic != MAGIC_NUMBER) {
            throw std::runtime_error("Invalid file format (bad magic number)");
        }

        uint32_t version;
        in.read(reinterpret_cast<char*>(&version), sizeof(version));

        uint32_t row_count, col_count;
        in.read(reinterpret_cast<char*>(&row_count), sizeof(row_count));
        in.read(reinterpret_cast<char*>(&col_count), sizeof(col_count));

        std::cout << "Loading " << row_count << " records from " << col_count << " columns (v" << version << ")...\n";

        // Read schema (v2 required)
        if (version < 2) {
            throw std::runtime_error(
                "Binary file version " + std::to_string(version) +
                " is not supported. Please re-compress your CSV data with the updated tool.");
        }

        std::vector<std::string> col_names(col_count);
        std::vector<ColumnType> col_types(col_count);

        for (uint32_t i = 0; i < col_count; ++i) {
            uint16_t name_len;
            in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
            col_names[i].resize(name_len);
            in.read(&col_names[i][0], name_len);

            uint8_t type_val;
            in.read(reinterpret_cast<char*>(&type_val), sizeof(type_val));
            col_types[i] = static_cast<ColumnType>(type_val);
        }

        // Read dictionaries
        uint32_t dict_count;
        in.read(reinterpret_cast<char*>(&dict_count), sizeof(dict_count));

        std::map<size_t, std::map<uint16_t, std::string>> reverse_dicts;

        for (uint32_t d = 0; d < dict_count; ++d) {
            uint32_t col_index;
            in.read(reinterpret_cast<char*>(&col_index), sizeof(col_index));

            uint32_t dict_size;
            in.read(reinterpret_cast<char*>(&dict_size), sizeof(dict_size));

            for (uint32_t i = 0; i < dict_size; ++i) {
                uint16_t id;
                in.read(reinterpret_cast<char*>(&id), sizeof(id));

                uint16_t len;
                in.read(reinterpret_cast<char*>(&len), sizeof(len));

                std::string value(len, '\0');
                in.read(&value[0], len);

                reverse_dicts[col_index][id] = value;
            }
        }


        // Map column names to ExperimentRecord fields using case-insensitive search
        auto find_col = [&](const std::vector<std::string>& patterns) -> int {
            for (uint32_t i = 0; i < col_count; ++i) {
                std::string lower_name = col_names[i];
                std::transform(lower_name.begin(), lower_name.end(),
                               lower_name.begin(), ::tolower);
                for (const auto& pattern : patterns) {
                    if (lower_name.find(pattern) != std::string::npos) {
                        return static_cast<int>(i);
                    }
                }
            }
            return -1;
        };

        int idx_experiment_id = find_col({"experimentid"});
        int idx_start = find_col({"starttime"});
        int idx_end = find_col({"endtime"});
        int idx_serial = find_col({"serialnumber", "instrumentserial"});
        int idx_desc = find_col({"instrumentdesc", "instrumentname"});
        int idx_user = find_col({"user"});
        int idx_source = find_col({"source"});

        // Decode rows
        std::vector<uint64_t> prev_timestamps(col_count, 0);
        std::vector<bool> first_timestamp(col_count, true);

        for (uint32_t row = 0; row < row_count && in.good(); ++row) {
            std::vector<std::string> values(col_count);
            std::vector<uint64_t> epoch_values(col_count, 0);

            for (uint32_t c = 0; c < col_count; ++c) {
                if (!in.good()) break;

                switch (col_types[c]) {
                    case ColumnType::CATEGORICAL: {
                        auto dict_it = reverse_dicts.find(c);
                        if (dict_it != reverse_dicts.end()) {
                            // Dictionary-encoded categorical
                            uint64_t dict_id = decode_variant(in);
                            auto val_it = dict_it->second.find(static_cast<uint16_t>(dict_id));
                            if (val_it != dict_it->second.end()) {
                                values[c] = val_it->second;
                            }
                        } else {
                            // No dictionary — stored as length-prefixed string
                            uint16_t len;
                            in.read(reinterpret_cast<char*>(&len), sizeof(len));
                            if (len > 0 && len < 10000) {
                                values[c].resize(len);
                                in.read(&values[c][0], len);
                            }
                        }
                        break;
                    }
                    case ColumnType::TIMESTAMP: {
                        uint64_t ts;
                        in.read(reinterpret_cast<char*>(&ts), sizeof(ts));
                        epoch_values[c] = ts;
                        prev_timestamps[c] = ts;
                        epoch_values[c] = ts;
                        values[c] = decode_timestamp(ts);
                        break;
                    }
                    case ColumnType::INTEGER: {
                        uint64_t val = decode_variant(in);
                        values[c] = std::to_string(val);
                        break;
                    }
                    case ColumnType::FLOAT: {
                        double val;
                        in.read(reinterpret_cast<char*>(&val), sizeof(val));
                        values[c] = std::to_string(val);
                        break;
                    }
                    case ColumnType::BOOLEAN: {
                        uint8_t val;
                        in.read(reinterpret_cast<char*>(&val), sizeof(val));
                        values[c] = val ? "true" : "false";
                        break;
                    }
                    case ColumnType::STRING:
                    default: {
                        uint16_t len;
                        in.read(reinterpret_cast<char*>(&len), sizeof(len));
                        if (len > 0 && len < 10000) { // sanity check
                            values[c].resize(len);
                            in.read(&values[c][0], len);
                        }
                        break;
                    }
                }
            }

            if (!in.good()) break;

            // Build ExperimentRecord from decoded row
            ExperimentRecord record;
            if (idx_experiment_id >= 0) record.experiment_id = values[idx_experiment_id];
            if (idx_start >= 0) {
                record.start_time = values[idx_start];
                record.start_epoch = epoch_values[idx_start];
            }
            if (idx_end >= 0) {
                record.end_time = values[idx_end];
                record.end_epoch = epoch_values[idx_end];
            }
            if (idx_serial >= 0) record.instrument_serial = values[idx_serial];
            if (idx_desc >= 0) record.instrument_desc = values[idx_desc];
            if (idx_user >= 0) record.user = values[idx_user];
            if (idx_source >= 0) record.source = values[idx_source];

            if (record.start_epoch > 0) {
                record.year = extract_year(record.start_epoch);
            }

            // Skip records with missing/invalid timestamps
            if (record.start_epoch == 0 || record.end_epoch == 0) continue;
            if (record.end_epoch < record.start_epoch) continue;

            // Sanity check: skip records with duration > 1 year (likely corrupt)
            double hours = calculate_duration_hours(record.start_epoch, record.end_epoch);
            if (hours > 8760.0) continue;

            records_.push_back(record);
        }

        std::cout << "Loaded " << records_.size() << " records.\n";
    }

    std::vector<std::string> QueryEngine::list_instruments() {
        std::set<std::string> unique;
        for (const auto& rec : records_) {
            std::string key = rec.instrument_serial.empty()
                ? rec.instrument_desc : rec.instrument_serial;
            if (!key.empty()) unique.insert(key);
        }
        return std::vector<std::string>(unique.begin(), unique.end());
    }

    std::vector<InstrumentStats> QueryEngine::get_instrument_utilization(
        const std::string& instrument_filter,
        int year
    ) {
        std::map<std::string, InstrumentStats> stats_map;

        for (const auto& rec : records_) {
            // Apply year filter
            if (year > 0 && rec.year != year) continue;

            // Determine instrument key (prefer serial, fall back to description)
            std::string key = rec.instrument_serial.empty()
                ? rec.instrument_desc : rec.instrument_serial;
            if (key.empty()) continue;

            // Apply instrument name filter
            if (!instrument_filter.empty() &&
                key.find(instrument_filter) == std::string::npos) continue;

            auto& stats = stats_map[key];
            stats.instrument = key;
            stats.experiment_count++;
            stats.total_hours += calculate_duration_hours(rec.start_epoch, rec.end_epoch);
        }

        // Calculate utilization as percentage of hours in a year (8760h)
        double period_hours = 8760.0;

        std::vector<InstrumentStats> results;
        for (auto& [key, stats] : stats_map) {
            stats.utilization_percent = (stats.total_hours / period_hours) * 100.0;
            results.push_back(stats);
        }

        // Sort by utilization descending
        std::sort(results.begin(), results.end(),
            [](const auto& a, const auto& b) {
                return a.utilization_percent > b.utilization_percent;
            });

        return results;
    }

    std::vector<InstrumentStats> QueryEngine::find_underutilized(
        double threshold_percent,
        int year
    ) {
        auto all_stats = get_instrument_utilization("", year);

        std::vector<InstrumentStats> results;
        for (const auto& stats : all_stats) {
            if (stats.utilization_percent < threshold_percent) {
                results.push_back(stats);
            }
        }
        return results;
    }

    double QueryEngine::calculate_duration_hours(uint64_t start_epoch, uint64_t end_epoch) {
        if (end_epoch <= start_epoch) return 0.0;
        return static_cast<double>(end_epoch - start_epoch) / 3600.0;
    }

} // namespace labcompress
