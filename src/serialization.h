#pragma once

#include "row.h"
#include "granule.h"
#include <string>
#include <vector>
#include <fstream>

namespace clickhouse {

class Serialization {
public:
    static void write_granule(const std::string& base_path, const Granule& granule, size_t granule_index);

    static Granule read_granule(const std::string& base_path, size_t granule_index);

    static void write_row_vector(const std::string& file_path, const RowVector& rows);

    static RowVector read_row_vector(const std::string& file_path);

    static void write_string_vector(const std::string& file_path, const std::vector<std::string>& strings);

    static std::vector<std::string> read_string_vector(const std::string& file_path);

    static void write_uint64_vector(const std::string& file_path, const std::vector<uint64_t>& values);

    static std::vector<uint64_t> read_uint64_vector(const std::string& file_path);

    static bool file_exists(const std::string& file_path);

    static size_t file_size(const std::string& file_path);

    static void write_string(std::ofstream& ofs, const std::string& str);

    static std::string read_string(std::ifstream& ifs);

    static void write_uint64(std::ofstream& ofs, uint64_t value);

    static uint64_t read_uint64(std::ifstream& ifs);

private:
};

}  // namespace clickhouse