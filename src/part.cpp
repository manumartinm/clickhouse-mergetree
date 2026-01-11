#include "part.h"
#include "serialization.h"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <stdexcept>

namespace clickhouse {

Part::Part(size_t part_id, const std::string& base_path)
    : metadata_(part_id), base_path_(base_path), loaded_(false) {
}

void Part::write_granules(const std::vector<Granule>& granules) {
    if (granules.empty()) {
        throw std::runtime_error("Cannot write empty granules");
    }

    create_directory();

    granules_ = granules;
    for (auto& granule : granules_) {
        granule.sort();
    }

    update_metadata(granules_);
    build_index(granules_);

    for (size_t i = 0; i < granules_.size(); ++i) {
        Serialization::write_granule(part_directory(), granules_[i], i);
    }

    save_index();
    save_metadata();
    loaded_ = true;
}

void Part::write_from_memtable_rows(const RowVector& rows) {
    if (rows.empty()) {
        throw std::runtime_error("Cannot write empty rows");
    }

    RowVector sorted_rows = rows;
    std::sort(sorted_rows.begin(), sorted_rows.end());

    std::vector<Granule> granules;
    Granule current_granule;

    for (const auto& row : sorted_rows) {
        if (current_granule.is_full()) {
            current_granule.sort();
            granules.push_back(std::move(current_granule));
            current_granule = Granule();
        }
        current_granule.add_row(row);
    }

    if (!current_granule.is_empty()) {
        current_granule.sort();
        granules.push_back(std::move(current_granule));
    }

    write_granules(granules);
}

RowVector Part::query(const std::string& start_key, const std::string& end_key) {
    if (!loaded_) {
        load();
    }

    RowVector result;

    if (!overlaps_range(start_key, end_key)) {
        return result;
    }

    auto granule_indices = index_.find_granules(start_key, end_key);

    for (size_t granule_idx : granule_indices) {
        if (granule_idx < granules_.size()) {
            auto granule_results = granules_[granule_idx].query_range(start_key, end_key);
            result.insert(result.end(), granule_results.begin(), granule_results.end());
        }
    }

    return result;
}

RowVector Part::query_key(const std::string& key) {
    return query(key, key);
}

void Part::load() {
    if (loaded_) {
        return;
    }

    if (!exists_on_disk()) {
        throw std::runtime_error("Part does not exist on disk: " + part_directory());
    }

    load_metadata();
    load_index();

    granules_.clear();
    granules_.reserve(metadata_.granule_count);

    for (size_t i = 0; i < metadata_.granule_count; ++i) {
        granules_.push_back(Serialization::read_granule(part_directory(), i));
    }

    loaded_ = true;
}

void Part::unload() {
    granules_.clear();
    loaded_ = false;
}

std::string Part::part_directory() const {
    return base_path_ + "/part_" + std::to_string(metadata_.part_id);
}

void Part::save_metadata() {
    std::string metadata_file = part_directory() + "/metadata.bin";
    std::ofstream ofs(metadata_file, std::ios::binary);

    if (!ofs) {
        throw std::runtime_error("Cannot save metadata: " + metadata_file);
    }

    Serialization::write_uint64(ofs, metadata_.part_id);
    Serialization::write_string(ofs, metadata_.min_key);
    Serialization::write_string(ofs, metadata_.max_key);
    Serialization::write_uint64(ofs, metadata_.min_timestamp);
    Serialization::write_uint64(ofs, metadata_.max_timestamp);
    Serialization::write_uint64(ofs, metadata_.row_count);
    Serialization::write_uint64(ofs, metadata_.granule_count);
    Serialization::write_uint64(ofs, metadata_.disk_size);
    Serialization::write_uint64(ofs, metadata_.creation_time);
}

void Part::load_metadata() {
    std::string metadata_file = part_directory() + "/metadata.bin";
    std::ifstream ifs(metadata_file, std::ios::binary);

    if (!ifs) {
        throw std::runtime_error("Cannot load metadata: " + metadata_file);
    }

    metadata_.part_id = Serialization::read_uint64(ifs);
    metadata_.min_key = Serialization::read_string(ifs);
    metadata_.max_key = Serialization::read_string(ifs);
    metadata_.min_timestamp = Serialization::read_uint64(ifs);
    metadata_.max_timestamp = Serialization::read_uint64(ifs);
    metadata_.row_count = Serialization::read_uint64(ifs);
    metadata_.granule_count = Serialization::read_uint64(ifs);
    metadata_.disk_size = Serialization::read_uint64(ifs);
    metadata_.creation_time = Serialization::read_uint64(ifs);
}

bool Part::exists_on_disk() const {
    return std::filesystem::exists(part_directory()) &&
           std::filesystem::exists(part_directory() + "/metadata.bin");
}

void Part::delete_from_disk() {
    if (exists_on_disk()) {
        std::filesystem::remove_all(part_directory());
    }
    unload();
}

size_t Part::disk_usage() const {
    if (!exists_on_disk()) {
        return 0;
    }

    size_t total = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(part_directory())) {
        if (entry.is_regular_file()) {
            total += entry.file_size();
        }
    }
    return total;
}

size_t Part::memory_usage() const {
    if (!loaded_) {
        return sizeof(Part) + sizeof(metadata_);
    }

    size_t total = sizeof(Part) + sizeof(metadata_) + index_.memory_usage();
    for (const auto& granule : granules_) {
        total += granule.memory_usage();
    }
    return total;
}

bool Part::overlaps_range(const std::string& start_key, const std::string& end_key) const {
    return !(metadata_.max_key < start_key || metadata_.min_key > end_key);
}

RowVector Part::get_all_rows() {
    if (!loaded_) {
        load();
    }

    RowVector result;
    for (const auto& granule : granules_) {
        const auto& rows = granule.rows();
        result.insert(result.end(), rows.begin(), rows.end());
    }

    return result;
}

void Part::update_metadata(const std::vector<Granule>& granules) {
    metadata_.granule_count = granules.size();
    metadata_.row_count = 0;
    metadata_.creation_time = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (granules.empty()) {
        return;
    }

    metadata_.min_key = granules.front().min_key();
    metadata_.max_key = granules.back().max_key();

    uint64_t min_ts = UINT64_MAX;
    uint64_t max_ts = 0;

    for (const auto& granule : granules) {
        metadata_.row_count += granule.size();

        for (const auto& row : granule.rows()) {
            min_ts = std::min(min_ts, row.timestamp);
            max_ts = std::max(max_ts, row.timestamp);
        }
    }

    metadata_.min_timestamp = min_ts;
    metadata_.max_timestamp = max_ts;
}

void Part::build_index(const std::vector<Granule>& granules) {
    index_.clear();

    for (size_t i = 0; i < granules.size(); ++i) {
        const auto& granule = granules[i];
        if (!granule.is_empty()) {
            index_.add_entry(granule.min_key(), granule.max_key(), i, granule.size());
        }
    }
}

void Part::save_index() {
    std::string index_file = part_directory() + "/primary.idx";
    index_.save_to_file(index_file);
}

void Part::load_index() {
    std::string index_file = part_directory() + "/primary.idx";
    index_.load_from_file(index_file);
}

void Part::create_directory() {
    std::filesystem::create_directories(part_directory());
}

}  // namespace clickhouse