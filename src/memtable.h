#pragma once

#include "row.h"
#include "granule.h"
#include <memory>
#include <vector>
#include <random>
#include <mutex>

namespace clickhouse {

class SkipListNode {
public:
    Row data;
    std::vector<std::shared_ptr<SkipListNode>> forward;

    SkipListNode(const Row& row, int level) : data(row), forward(level + 1) {}
};

class MemTable {
private:
    static constexpr int MAX_LEVEL = 16;
    static constexpr double PROBABILITY = 0.5;

    std::shared_ptr<SkipListNode> header_;
    int current_level_;
    size_t size_;
    size_t memory_usage_;
    std::mt19937 rng_;
    mutable std::mutex mutex_;

public:
    MemTable();

    void insert(const Row& row);

    RowVector query(const std::string& start_key, const std::string& end_key) const;

    RowVector query_key(const std::string& key) const;

    bool empty() const;

    size_t size() const;

    size_t memory_usage() const;

    void clear();

    std::vector<Granule> flush_to_granules();

    RowVector get_all_rows() const;

private:
    int random_level();

    std::shared_ptr<SkipListNode> find_node(const std::string& key) const;

    void update_memory_usage(const Row& row);
};

}  // namespace clickhouse