#include "schemaparser.hpp"
#include "compressor.hpp"
#include "queryengine.hpp"
#include <iostream>
#include <iomanip>
#include <vector>

void print_utilization_report(const std::vector<labcompress::InstrumentStats>& stats) {
    std::cout << "\n";
    std::cout << std::left << std::setw(30) << "Instrument"
              << std::right << std::setw(12) << "Experiments"
              << std::setw(15) << "Runtime (hrs)"
              << std::setw(15) << "Utilization %"
              << "\n";
    std::cout << std::string(72, '-') << "\n";
    
    for (const auto& s : stats) {
        std::cout << std::left << std::setw(30) << s.instrument
                  << std::right << std::setw(12) << s.experiment_count
                  << std::setw(15) << std::fixed << std::setprecision(1) << s.total_hours
                  << std::setw(14) << std::fixed << std::setprecision(1) << s.utilization_percent << "%"
                  << "\n";
    }
    std::cout << "\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage:\n";
        std::cerr << "  analyze <csv_file>\n";
        std::cerr << "  compress <csv_file> <output_file>\n";
        std::cerr << "  query <binary_file> instrument [name] [year]\n";
        std::cerr << "  query <binary_file> underutilized <threshold> <year>\n";
        return 1;
    }
    
    std::string command = argv[1];
    
    try {
        if (command == "analyze") {
            auto schema = labcompress::DataSchema::from_csv(argv[2]);
            schema.analyze_compression_opportunities();
        }
        else if (command == "compress") {
            if (argc < 4) {
                std::cerr << "Error: compress requires output filename\n";
                return 1;
            }
            auto schema = labcompress::DataSchema::from_csv(argv[2]);
            labcompress::Compressor compressor(schema);
            compressor.compress(argv[2], argv[3]);
        }
        else if (command == "query") {
            if (argc < 4) {
                std::cerr << "Error: query requires binary file and query type\n";
                return 1;
            }
            
            labcompress::QueryEngine engine(argv[2]);
            std::string query_type = argv[3];
            
            if (query_type == "instrument") {
                std::string filter = (argc > 4) ? argv[4] : "";
                int year = (argc > 5) ? std::stoi(argv[5]) : 0;
                
                auto stats = engine.get_instrument_utilization(filter, year);
                print_utilization_report(stats);
            }
            else if (query_type == "underutilized") {
                if (argc < 6) {
                    std::cerr << "Usage: query <file> underutilized <threshold> <year>\n";
                    return 1;
                }
                double threshold = std::stod(argv[4]);
                int year = std::stoi(argv[5]);
                
                auto stats = engine.find_underutilized(threshold, year);
                std::cout << "\nUnderutilized Instruments (<" << threshold << "% in " << year << "):\n";
                print_utilization_report(stats);
            }
        }
        else {
            std::cerr << "Unknown command: " << command << "\n";
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}