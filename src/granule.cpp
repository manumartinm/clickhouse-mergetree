#include "granule.h"
#include <algorithm>
#include <stdexcept>

namespace clickhouse {

Granule::Granule() : sorted_(false) {
    rows_.reserve(GRANULE_SIZE);
}

void Granule::add_row(const Row& row) {
    if (is_full()) {
        throw std::runtime_error("Granule is full, cannot add more rows");
    }

    rows_.push_back(row);
    sorted_ = false;
    update_key_range();
}

bool Granule::is_full() const {
    return rows_.size() >= GRANULE_SIZE;
}

bool Granule::is_empty() const {
    return rows_.empty();
}

size_t Granule::size() const {
    return rows_.size();
}

void Granule::sort() {
    if (!sorted_) {
        std::sort(rows_.begin(), rows_.end());
        sorted_ = true;
        update_key_range();
    }
}

void Granule::clear() {
    rows_.clear();
    min_key_.clear();
    max_key_.clear();
    sorted_ = false;
}

RowVector Granule::query_range(const std::string& start_key, const std::string& end_key) const {
    if (!sorted_) {
        throw std::runtime_error("Granule must be sorted before querying");
    }

    RowVector result;
    result.reserve(rows_.size());

    for (const auto& row : rows_) {
        if (row.key >= start_key && row.key <= end_key) {
            result.push_back(row);
        } else if (row.key > end_key) {
            break;
        }
    }

    return result;
}

size_t Granule::memory_usage() const {
    size_t total = sizeof(Granule);
    for (const auto& row : rows_) {
        total += row.size();
    }
    return total;
}

void Granule::update_key_range() {
    if (rows_.empty()) {
        min_key_.clear();
        max_key_.clear();
        return;
    }

    if (sorted_) {
        min_key_ = rows_.front().key;
        max_key_ = rows_.back().key;
    } else {
        auto min_it = std::min_element(rows_.begin(), rows_.end(),
            [](const Row& a, const Row& b) { return a.key < b.key; });
        auto max_it = std::max_element(rows_.begin(), rows_.end(),
            [](const Row& a, const Row& b) { return a.key < b.key; });

        min_key_ = min_it->key;
        max_key_ = max_it->key;
    }
}

}  // namespace clickhouse