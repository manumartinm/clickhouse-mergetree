#include "sparse_index.h"
#include "serialization.h"
#include <algorithm>
#include <fstream>

namespace clickhouse {

void SparseIndex::add_entry(const std::string& min_key, const std::string& max_key,
                           size_t granule_index, size_t row_count) {
    entries_.emplace_back(min_key, max_key, granule_index, row_count);
}

void SparseIndex::add_entry(const IndexEntry& entry) {
    entries_.push_back(entry);
}

std::vector<size_t> SparseIndex::find_granules(const std::string& start_key, const std::string& end_key) const {
    std::vector<size_t> result;

    for (const auto& entry : entries_) {
        if (entry.overlaps_range(start_key, end_key)) {
            result.push_back(entry.granule_index);
        }
    }

    return result;
}

std::vector<size_t> SparseIndex::find_granules_for_key(const std::string& key) const {
    return find_granules(key, key);
}

void SparseIndex::clear() {
    entries_.clear();
}

bool SparseIndex::empty() const {
    return entries_.empty();
}

size_t SparseIndex::size() const {
    return entries_.size();
}

void SparseIndex::save_to_file(const std::string& file_path) const {
    std::ofstream ofs(file_path, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Cannot open file for writing: " + file_path);
    }

    Serialization::write_uint64(ofs, entries_.size());

    for (const auto& entry : entries_) {
        Serialization::write_string(ofs, entry.min_key);
        Serialization::write_string(ofs, entry.max_key);
        Serialization::write_uint64(ofs, entry.granule_index);
        Serialization::write_uint64(ofs, entry.row_count);
    }
}

void SparseIndex::load_from_file(const std::string& file_path) {
    std::ifstream ifs(file_path, std::ios::binary);
    if (!ifs) {
        throw std::runtime_error("Cannot open file for reading: " + file_path);
    }

    entries_.clear();

    uint64_t count = Serialization::read_uint64(ifs);
    entries_.reserve(count);

    for (uint64_t i = 0; i < count; ++i) {
        std::string min_key = Serialization::read_string(ifs);
        std::string max_key = Serialization::read_string(ifs);
        uint64_t granule_index = Serialization::read_uint64(ifs);
        uint64_t row_count = Serialization::read_uint64(ifs);

        entries_.emplace_back(min_key, max_key, granule_index, row_count);
    }
}

void SparseIndex::merge_with(const SparseIndex& other, size_t granule_offset) {
    for (const auto& entry : other.entries_) {
        IndexEntry new_entry = entry;
        new_entry.granule_index += granule_offset;
        entries_.push_back(new_entry);
    }

    sort_entries();
}

size_t SparseIndex::memory_usage() const {
    size_t total = sizeof(SparseIndex);
    for (const auto& entry : entries_) {
        total += sizeof(IndexEntry) + entry.min_key.size() + entry.max_key.size();
    }
    return total;
}

void SparseIndex::sort_entries() {
    std::sort(entries_.begin(), entries_.end(),
        [](const IndexEntry& a, const IndexEntry& b) {
            if (a.min_key != b.min_key) return a.min_key < b.min_key;
            return a.granule_index < b.granule_index;
        });
}

}  // namespace clickhouse