#pragma once

#include "row.h"
#include <vector>
#include <string>
#include <algorithm>

namespace clickhouse {

constexpr size_t GRANULE_SIZE = 8192;  // ClickHouse default granule size

class Granule {
private:
    RowVector rows_;
    std::string min_key_;
    std::string max_key_;
    bool sorted_;

public:
    Granule();

    void add_row(const Row& row);
    bool is_full() const;
    bool is_empty() const;
    size_t size() const;
    void sort();

    const std::string& min_key() const { return min_key_; }
    const std::string& max_key() const { return max_key_; }

    const RowVector& rows() const { return rows_; }
    RowVector& rows() { return rows_; }

    void clear();

    RowVector query_range(const std::string& start_key, const std::string& end_key) const;

    size_t memory_usage() const;

private:
    void update_key_range();
};

}  // namespace clickhouse