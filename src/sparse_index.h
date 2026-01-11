#pragma once

#include "row.h"
#include <vector>
#include <string>

namespace clickhouse {

struct IndexEntry {
    std::string min_key;
    std::string max_key;
    size_t granule_index;
    size_t row_count;

    IndexEntry() = default;
    IndexEntry(const std::string& min_k, const std::string& max_k, size_t idx, size_t count)
        : min_key(min_k), max_key(max_k), granule_index(idx), row_count(count) {}

    bool overlaps_range(const std::string& start_key, const std::string& end_key) const {
        return !(max_key < start_key || min_key > end_key);
    }
};

class SparseIndex {
private:
    std::vector<IndexEntry> entries_;

public:
    SparseIndex() = default;

    void add_entry(const std::string& min_key, const std::string& max_key,
                   size_t granule_index, size_t row_count);

    void add_entry(const IndexEntry& entry);

    std::vector<size_t> find_granules(const std::string& start_key, const std::string& end_key) const;

    std::vector<size_t> find_granules_for_key(const std::string& key) const;

    void clear();

    bool empty() const;

    size_t size() const;

    const std::vector<IndexEntry>& entries() const { return entries_; }

    void save_to_file(const std::string& file_path) const;

    void load_from_file(const std::string& file_path);

    void merge_with(const SparseIndex& other, size_t granule_offset);

    size_t memory_usage() const;

private:
    void sort_entries();
};

}  // namespace clickhouse