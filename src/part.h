#pragma once

#include "row.h"
#include "granule.h"
#include "sparse_index.h"
#include <string>
#include <vector>
#include <memory>

namespace clickhouse {

struct PartMetadata {
    size_t part_id;
    std::string min_key;
    std::string max_key;
    uint64_t min_timestamp;
    uint64_t max_timestamp;
    size_t row_count;
    size_t granule_count;
    size_t disk_size;
    uint64_t creation_time;

    PartMetadata() = default;
    PartMetadata(size_t id) : part_id(id), row_count(0), granule_count(0),
                              disk_size(0), creation_time(0) {}
};

class Part {
private:
    PartMetadata metadata_;
    std::string base_path_;
    std::vector<Granule> granules_;
    SparseIndex index_;
    bool loaded_;

public:
    Part(size_t part_id, const std::string& base_path);

    void write_granules(const std::vector<Granule>& granules);

    void write_from_memtable_rows(const RowVector& rows);

    RowVector query(const std::string& start_key, const std::string& end_key);

    RowVector query_key(const std::string& key);

    void load();

    void unload();

    bool is_loaded() const { return loaded_; }

    const PartMetadata& metadata() const { return metadata_; }

    const SparseIndex& index() const { return index_; }

    std::string part_directory() const;

    void save_metadata();

    void load_metadata();

    bool exists_on_disk() const;

    void delete_from_disk();

    size_t disk_usage() const;

    size_t memory_usage() const;

    bool overlaps_range(const std::string& start_key, const std::string& end_key) const;

    RowVector get_all_rows();

private:
    void update_metadata(const std::vector<Granule>& granules);

    void build_index(const std::vector<Granule>& granules);

    void save_index();

    void load_index();

    void create_directory();
};

}  // namespace clickhouse