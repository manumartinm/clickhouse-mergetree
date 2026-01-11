#pragma once

#include "row.h"
#include "memtable.h"
#include "part.h"
#include "merger.h"
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace clickhouse {

struct MergeTreeConfig {
    size_t memtable_flush_threshold = 1000;
    size_t max_parts = 10;
    size_t merge_interval_seconds = 30;
    bool enable_background_merge = true;

    MergeTreeConfig() = default;
};

class MergeTree {
private:
    MergeTreeConfig config_;
    std::string base_path_;

    MemTable memtable_;
    std::vector<std::unique_ptr<Part>> parts_;
    Merger merger_;

    mutable std::mutex parts_mutex_;
    mutable std::mutex memtable_mutex_;

    std::thread background_thread_;
    std::atomic<bool> shutdown_;
    std::condition_variable background_cv_;
    std::mutex background_mutex_;

public:
    explicit MergeTree(const std::string& base_path, const MergeTreeConfig& config = MergeTreeConfig());

    ~MergeTree();

    void insert(const std::string& key, const std::string& value, uint64_t timestamp);

    void insert(const Row& row);

    RowVector query(const std::string& start_key, const std::string& end_key);

    RowVector query_key(const std::string& key);

    void flush_memtable();

    void merge_parts_sync();

    void shutdown();

    size_t part_count() const;

    size_t total_rows() const;

    size_t memory_usage() const;

    size_t disk_usage() const;

    void load_existing_parts();

    void optimize();

private:
    void background_merge_worker();

    void trigger_flush_if_needed();

    bool should_trigger_merge() const;

    void perform_merge();

    size_t get_next_part_id() const;

    void create_base_directory();
};

}  // namespace clickhouse