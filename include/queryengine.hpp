#pragma once
#include "schemaparser.hpp"
#include <string>
#include <vector>
#include <map>

namespace labcompress {

    struct ExperimentRecord {
        std::string experiment_id;
        std::string start_time;
        std::string end_time;
        uint64_t start_epoch = 0;
        uint64_t end_epoch = 0;
        int year = 0;
        std::string instrument_serial;
        std::string instrument_desc;
        std::string user;
        std::string source;
    };

    struct InstrumentStats {
        std::string instrument; 
        uint32_t experiment_count = 0; 
        double total_hours = 0.0; 
        double utilization_percent = 0.0; 
    }; 

    class QueryEngine {
        public: 
            QueryEngine(const std::string& binary_file); 

            // Query: Show instrument util 
            std::vector<InstrumentStats> get_instrument_utilization(
                const std::string& instrument_filter = "", 
                int year = 0 
            );

            // Query: Find underutilized instruments
            std::vector<InstrumentStats> find_underutilized ( 
                double threshold_percent, 
                int year = 0
            );

            // List all unique instruments
            std::vector<std::string> list_instruments();

        private: 
            std::string binary_file_;
            std::vector<ExperimentRecord> records_;

            void load_data();
            uint64_t decode_variant(std::ifstream& in);
            double calculate_duration_hours(uint64_t start_epoch, uint64_t end_epoch);
    };
}