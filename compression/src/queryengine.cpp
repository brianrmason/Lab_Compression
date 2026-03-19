#include "../include/queryengine.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>

namespace labcompress {

    uint64_t QueryEngine::decode_variant(std::ifstream& in) {
        uint64_t result = 0; 
        int shift = 0; 

        while (true) {
            char byte; 
            in.read(&byte, 1); 
            result |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                break; 
            } 
            shift += 7;
        }
        return result;
    }

    std::string decode_timestamp(uint64_t epoch_seconds) {
        std::time_t t = static_cast<std::time_t>(epoch_seconds);
        std::tm* tm = std::localtime(&t); 

        std::ostringstream oss;
        oss << (tm->tm_mon + 1) << "/" << tm->tm_mday << "/" << (tm->tm_year + 1900)
        << " " << tm->tm_hour << ":" << std::setfill('0') << std::setw(2) << tm->tm_min;
        return oss.str();
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

        std::cout << "Loading compressed file... \n";

        // Read magic number
        uint32_t magic;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (magic != 0x5554494C) {
            throw std::runtime_error("Invalid file format (bad magic number)");
        }

        // Read version 
        uint32_t version; 
        in.read(reinterpret_cast<char*>(&version), sizeof(version));

        // Read counts
        uint32_t row_count, col_count;
        in.read(reinterpret_cast<char*>(&row_count), sizeof(row_count));
        in.read(reinterpret_cast<char*>(&col_count), sizeof(col_count));

        std::cout << "File header: " << row_count << " rows (sample), " << col_count << " columns\n";

        // Read dictionary 
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

        std::cout << "Dictionaries loaded: " << dict_count << " dictionaries\n"; 

        in.close(); 

        std::cout << "Decompression complete! Data ready for querying. \n\n"; 

        const size_t COL_START_TIME = 7;
        const size_t COL_END_TIME = 8;
        const size_t COL_INSTRUMENT_SERIAL = 18;
        const size_t COL_INSTRUMENT_DESC = 17;
        const size_t COL_USER = 12;
        const size_t COL_SOURCE = 10;
        
        uint64_t prev_start_time = 0;
        uint64_t prev_end_time = 0;

        int row_num = 0; 
        while (!in.eof() && row_num < 20000) {
            ExperimentRecord record;

            try {
                
            }
        }

    }

    std::vector<std::string> QueryEngine::list_instruments() {
        return {"OptiMax1001", "EasyMax102", "Reactor5"};
    }

    std::vector<InstrumentStats> QueryEngine::get_instrument_utilization(
        const std::string& instrument_filter,
        int year
    ) {
        std::vector<InstrumentStats> results; 

        // Placeholder 
        InstrumentStats stat;
        stat.instrument = "OptiMax1001";
        stat.experiment_count = 156;
        stat.total_hours = 3456.8;
        stat.utilization_percent = 65.4;
        results.push_back(stat);
        
        return results;
    }

    std::vector<InstrumentStats> QueryEngine::find_underutilized(
        double threshold_percent,
        int year
    ) {
        std::vector<InstrumentStats> results;
        
        // Placeholder
        InstrumentStats stat;
        stat.instrument = "EasyMax102";
        stat.experiment_count = 89;
        stat.total_hours = 1465.8;
        stat.utilization_percent = 23.4;
        results.push_back(stat);
        
        return results;
    }


    double QueryEngine::calculate_duration_hours(const std::string& start, const std::string& end) {
        // Placeholder - return 0 for now
        return 0.0;
    }

} // namespace labcompress