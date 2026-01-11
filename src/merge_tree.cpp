#include "merge_tree.h"
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <iostream>

namespace clickhouse {

MergeTree::MergeTree(const std::string& base_path, const MergeTreeConfig& config)
    : config_(config), base_path_(base_path), merger_(base_path), shutdown_(false) {

    create_base_directory();
    load_existing_parts();

    if (config_.enable_background_merge) {
        background_thread_ = std::thread(&MergeTree::background_merge_worker, this);
    }
}

MergeTree::~MergeTree() {
    shutdown();
}

void MergeTree::insert(const std::string& key, const std::string& value, uint64_t timestamp) {
    insert(Row(key, value, timestamp));
}

void MergeTree::insert(const Row& row) {
    {
        std::lock_guard<std::mutex> lock(memtable_mutex_);
        memtable_.insert(row);
    }

    trigger_flush_if_needed();
}

RowVector MergeTree::query(const std::string& start_key, const std::string& end_key) {
    RowVector result;

    {
        std::lock_guard<std::mutex> lock(memtable_mutex_);
        auto memtable_results = memtable_.query(start_key, end_key);
        result.insert(result.end(), memtable_results.begin(), memtable_results.end());
    }

    {
        std::lock_guard<std::mutex> lock(parts_mutex_);
        for (auto& part : parts_) {
            if (part->overlaps_range(start_key, end_key)) {
                auto part_results = part->query(start_key, end_key);
                result.insert(result.end(), part_results.begin(), part_results.end());
            }
        }
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end(),
        [](const Row& a, const Row& b) {
            return a.key == b.key && a.timestamp == b.timestamp;
        }), result.end());

    return result;
}

RowVector MergeTree::query_key(const std::string& key) {
    return query(key, key);
}

void MergeTree::flush_memtable() {
    RowVector rows;
    {
        std::lock_guard<std::mutex> lock(memtable_mutex_);
        if (memtable_.empty()) {
            return;
        }
        rows = memtable_.get_all_rows();
        memtable_.clear();
    }

    if (rows.empty()) {
        return;
    }

    auto new_part = std::make_unique<Part>(get_next_part_id(), base_path_);
    new_part->write_from_memtable_rows(rows);

    {
        std::lock_guard<std::mutex> lock(parts_mutex_);
        parts_.push_back(std::move(new_part));
    }
}

void MergeTree::merge_parts_sync() {
    if (should_trigger_merge()) {
        perform_merge();
    }
}

void MergeTree::shutdown() {
    if (!shutdown_.exchange(true)) {
        {
            std::lock_guard<std::mutex> lock(background_mutex_);
        }
        background_cv_.notify_all();

        if (background_thread_.joinable()) {
            background_thread_.join();
        }

        flush_memtable();
    }
}

size_t MergeTree::part_count() const {
    std::lock_guard<std::mutex> lock(parts_mutex_);
    return parts_.size();
}

size_t MergeTree::total_rows() const {
    size_t total = 0;

    {
        std::lock_guard<std::mutex> lock(memtable_mutex_);
        total += memtable_.size();
    }

    {
        std::lock_guard<std::mutex> lock(parts_mutex_);
        for (const auto& part : parts_) {
            total += part->metadata().row_count;
        }
    }

    return total;
}

size_t MergeTree::memory_usage() const {
    size_t total = sizeof(MergeTree);

    {
        std::lock_guard<std::mutex> lock(memtable_mutex_);
        total += memtable_.memory_usage();
    }

    {
        std::lock_guard<std::mutex> lock(parts_mutex_);
        for (const auto& part : parts_) {
            total += part->memory_usage();
        }
    }

    return total;
}

size_t MergeTree::disk_usage() const {
    size_t total = 0;
    std::lock_guard<std::mutex> lock(parts_mutex_);
    for (const auto& part : parts_) {
        total += part->disk_usage();
    }
    return total;
}

void MergeTree::load_existing_parts() {
    if (!std::filesystem::exists(base_path_)) {
        return;
    }

    std::vector<size_t> part_ids;

    for (const auto& entry : std::filesystem::directory_iterator(base_path_)) {
        if (entry.is_directory()) {
            std::string dirname = entry.path().filename().string();
            if (dirname.substr(0, 5) == "part_") {
                std::string id_str = dirname.substr(5);
                try {
                    size_t part_id = std::stoull(id_str);
                    part_ids.push_back(part_id);
                } catch (...) {
                }
            }
        }
    }

    std::sort(part_ids.begin(), part_ids.end());

    for (size_t part_id : part_ids) {
        auto part = std::make_unique<Part>(part_id, base_path_);
        if (part->exists_on_disk()) {
            parts_.push_back(std::move(part));
        }
    }

    if (!part_ids.empty()) {
        merger_.set_next_part_id(part_ids.back() + 1);
    }
}

void MergeTree::optimize() {
    flush_memtable();

    while (should_trigger_merge()) {
        perform_merge();
    }
}

void MergeTree::background_merge_worker() {
    while (!shutdown_) {
        {
            std::unique_lock<std::mutex> lock(background_mutex_);
            background_cv_.wait_for(lock, std::chrono::seconds(config_.merge_interval_seconds),
                [this] { return shutdown_.load(); });
        }

        if (!shutdown_) {
            try {
                trigger_flush_if_needed();
                if (should_trigger_merge()) {
                    perform_merge();
                }
            } catch (const std::exception& e) {
                std::cerr << "Background merge error: " << e.what() << std::endl;
            }
        }
    }
}

void MergeTree::trigger_flush_if_needed() {
    bool should_flush = false;
    {
        std::lock_guard<std::mutex> lock(memtable_mutex_);
        should_flush = memtable_.size() >= config_.memtable_flush_threshold;
    }

    if (should_flush) {
        flush_memtable();
    }
}

bool MergeTree::should_trigger_merge() const {
    std::lock_guard<std::mutex> lock(parts_mutex_);
    return parts_.size() > config_.max_parts;
}

void MergeTree::perform_merge() {
    std::vector<std::unique_ptr<Part>> parts_to_merge;

    {
        std::lock_guard<std::mutex> lock(parts_mutex_);

        if (parts_.size() < 2) {
            return;
        }

        auto candidates = merger_.select_merge_candidates(parts_, 1);

        if (candidates.empty()) {
            return;
        }

        const auto& best_candidate = candidates[0];

        std::vector<std::unique_ptr<Part>> remaining_parts;

        for (size_t i = 0; i < parts_.size(); ++i) {
            bool is_selected = std::find(best_candidate.part_indices.begin(),
                                       best_candidate.part_indices.end(), i) !=
                              best_candidate.part_indices.end();

            if (is_selected) {
                parts_to_merge.push_back(std::move(parts_[i]));
            } else {
                remaining_parts.push_back(std::move(parts_[i]));
            }
        }

        parts_ = std::move(remaining_parts);
    }

    if (!parts_to_merge.empty()) {
        auto merged_part = merger_.merge_parts(std::move(parts_to_merge));

        {
            std::lock_guard<std::mutex> lock(parts_mutex_);
            parts_.push_back(std::move(merged_part));
        }
    }
}

size_t MergeTree::get_next_part_id() const {
    size_t id = merger_.get_next_part_id();
    const_cast<Merger&>(merger_).set_next_part_id(id + 1);
    return id;
}

void MergeTree::create_base_directory() {
    std::filesystem::create_directories(base_path_);
}

}  // namespace clickhouse