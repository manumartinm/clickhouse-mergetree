#include "serialization.h"
#include <filesystem>
#include <stdexcept>
#include <iostream>

namespace clickhouse {

void Serialization::write_granule(const std::string& base_path, const Granule& granule, size_t granule_index) {
    const auto& rows = granule.rows();

    std::vector<std::string> keys, values;
    std::vector<uint64_t> timestamps;

    keys.reserve(rows.size());
    values.reserve(rows.size());
    timestamps.reserve(rows.size());

    for (const auto& row : rows) {
        keys.push_back(row.key);
        values.push_back(row.value);
        timestamps.push_back(row.timestamp);
    }

    std::string granule_prefix = base_path + "/granule_" + std::to_string(granule_index);

    write_string_vector(granule_prefix + "_keys.bin", keys);
    write_string_vector(granule_prefix + "_values.bin", values);
    write_uint64_vector(granule_prefix + "_timestamps.bin", timestamps);
}

Granule Serialization::read_granule(const std::string& base_path, size_t granule_index) {
    std::string granule_prefix = base_path + "/granule_" + std::to_string(granule_index);

    auto keys = read_string_vector(granule_prefix + "_keys.bin");
    auto values = read_string_vector(granule_prefix + "_values.bin");
    auto timestamps = read_uint64_vector(granule_prefix + "_timestamps.bin");

    if (keys.size() != values.size() || keys.size() != timestamps.size()) {
        throw std::runtime_error("Inconsistent granule data sizes");
    }

    Granule granule;
    for (size_t i = 0; i < keys.size(); ++i) {
        granule.add_row(Row(keys[i], values[i], timestamps[i]));
    }

    granule.sort();
    return granule;
}

void Serialization::write_row_vector(const std::string& file_path, const RowVector& rows) {
    std::ofstream ofs(file_path, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Cannot open file for writing: " + file_path);
    }

    write_uint64(ofs, rows.size());

    for (const auto& row : rows) {
        write_string(ofs, row.key);
        write_string(ofs, row.value);
        write_uint64(ofs, row.timestamp);
    }
}

RowVector Serialization::read_row_vector(const std::string& file_path) {
    std::ifstream ifs(file_path, std::ios::binary);
    if (!ifs) {
        throw std::runtime_error("Cannot open file for reading: " + file_path);
    }

    uint64_t count = read_uint64(ifs);
    RowVector rows;
    rows.reserve(count);

    for (uint64_t i = 0; i < count; ++i) {
        std::string key = read_string(ifs);
        std::string value = read_string(ifs);
        uint64_t timestamp = read_uint64(ifs);
        rows.emplace_back(key, value, timestamp);
    }

    return rows;
}

void Serialization::write_string_vector(const std::string& file_path, const std::vector<std::string>& strings) {
    std::ofstream ofs(file_path, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Cannot open file for writing: " + file_path);
    }

    write_uint64(ofs, strings.size());

    for (const auto& str : strings) {
        write_string(ofs, str);
    }
}

std::vector<std::string> Serialization::read_string_vector(const std::string& file_path) {
    std::ifstream ifs(file_path, std::ios::binary);
    if (!ifs) {
        throw std::runtime_error("Cannot open file for reading: " + file_path);
    }

    uint64_t count = read_uint64(ifs);
    std::vector<std::string> strings;
    strings.reserve(count);

    for (uint64_t i = 0; i < count; ++i) {
        strings.push_back(read_string(ifs));
    }

    return strings;
}

void Serialization::write_uint64_vector(const std::string& file_path, const std::vector<uint64_t>& values) {
    std::ofstream ofs(file_path, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Cannot open file for writing: " + file_path);
    }

    write_uint64(ofs, values.size());

    for (uint64_t value : values) {
        write_uint64(ofs, value);
    }
}

std::vector<uint64_t> Serialization::read_uint64_vector(const std::string& file_path) {
    std::ifstream ifs(file_path, std::ios::binary);
    if (!ifs) {
        throw std::runtime_error("Cannot open file for reading: " + file_path);
    }

    uint64_t count = read_uint64(ifs);
    std::vector<uint64_t> values;
    values.reserve(count);

    for (uint64_t i = 0; i < count; ++i) {
        values.push_back(read_uint64(ifs));
    }

    return values;
}

bool Serialization::file_exists(const std::string& file_path) {
    return std::filesystem::exists(file_path);
}

size_t Serialization::file_size(const std::string& file_path) {
    if (!file_exists(file_path)) {
        return 0;
    }
    return std::filesystem::file_size(file_path);
}

void Serialization::write_string(std::ofstream& ofs, const std::string& str) {
    uint64_t length = str.size();
    write_uint64(ofs, length);
    ofs.write(str.data(), length);
}

std::string Serialization::read_string(std::ifstream& ifs) {
    uint64_t length = read_uint64(ifs);
    std::string str(length, '\0');
    ifs.read(&str[0], length);
    return str;
}

void Serialization::write_uint64(std::ofstream& ofs, uint64_t value) {
    ofs.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

uint64_t Serialization::read_uint64(std::ifstream& ifs) {
    uint64_t value;
    ifs.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

}  // namespace clickhouse