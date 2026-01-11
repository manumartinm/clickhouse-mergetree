#pragma once

#include <string>
#include <cstdint>
#include <vector>

namespace clickhouse {

struct Row {
    std::string key;
    std::string value;
    uint64_t timestamp;

    Row() = default;
    Row(const std::string& k, const std::string& v, uint64_t ts)
        : key(k), value(v), timestamp(ts) {}

    bool operator<(const Row& other) const {
        if (key != other.key) return key < other.key;
        return timestamp < other.timestamp;
    }

    bool operator==(const Row& other) const {
        return key == other.key && value == other.value && timestamp == other.timestamp;
    }

    size_t size() const {
        return sizeof(timestamp) + key.size() + value.size();
    }
};

using RowVector = std::vector<Row>;

}  // namespace clickhouse