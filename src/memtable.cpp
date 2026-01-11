#include "memtable.h"
#include <algorithm>
#include <chrono>

namespace clickhouse {

MemTable::MemTable()
    : current_level_(0), size_(0), memory_usage_(0),
      rng_(std::chrono::steady_clock::now().time_since_epoch().count()) {

    Row dummy_row("", "", 0);
    header_ = std::make_shared<SkipListNode>(dummy_row, MAX_LEVEL);
}

void MemTable::insert(const Row& row) {
    std::lock_guard<std::mutex> lock(mutex_);

    int level = random_level();
    auto new_node = std::make_shared<SkipListNode>(row, level);

    if (level > current_level_) {
        for (int i = current_level_ + 1; i <= level; ++i) {
            header_->forward[i] = nullptr;
        }
        current_level_ = level;
    }

    std::vector<std::shared_ptr<SkipListNode>> update(MAX_LEVEL + 1);
    auto current = header_;

    for (int i = current_level_; i >= 0; --i) {
        while (current->forward[i] && current->forward[i]->data < row) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    for (int i = 0; i <= level; ++i) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }

    size_++;
    update_memory_usage(row);
}

RowVector MemTable::query(const std::string& start_key, const std::string& end_key) const {
    std::lock_guard<std::mutex> lock(mutex_);

    RowVector result;
    auto current = header_->forward[0];

    while (current) {
        if (current->data.key >= start_key && current->data.key <= end_key) {
            result.push_back(current->data);
        } else if (current->data.key > end_key) {
            break;
        }
        current = current->forward[0];
    }

    return result;
}

RowVector MemTable::query_key(const std::string& key) const {
    return query(key, key);
}

bool MemTable::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_ == 0;
}

size_t MemTable::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_;
}

size_t MemTable::memory_usage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return memory_usage_;
}

void MemTable::clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    Row dummy_row("", "", 0);
    header_ = std::make_shared<SkipListNode>(dummy_row, MAX_LEVEL);
    current_level_ = 0;
    size_ = 0;
    memory_usage_ = 0;
}

std::vector<Granule> MemTable::flush_to_granules() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<Granule> granules;
    if (size_ == 0) {
        return granules;
    }

    Granule current_granule;
    auto current = header_->forward[0];

    while (current) {
        if (current_granule.is_full()) {
            current_granule.sort();
            granules.push_back(std::move(current_granule));
            current_granule = Granule();
        }

        current_granule.add_row(current->data);
        current = current->forward[0];
    }

    if (!current_granule.is_empty()) {
        current_granule.sort();
        granules.push_back(std::move(current_granule));
    }

    return granules;
}

RowVector MemTable::get_all_rows() const {
    std::lock_guard<std::mutex> lock(mutex_);

    RowVector result;
    result.reserve(size_);

    auto current = header_->forward[0];
    while (current) {
        result.push_back(current->data);
        current = current->forward[0];
    }

    return result;
}

int MemTable::random_level() {
    int level = 0;
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    while (dist(rng_) < PROBABILITY && level < MAX_LEVEL) {
        level++;
    }

    return level;
}

std::shared_ptr<SkipListNode> MemTable::find_node(const std::string& key) const {
    auto current = header_;

    for (int i = current_level_; i >= 0; --i) {
        while (current->forward[i] && current->forward[i]->data.key < key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];
    if (current && current->data.key == key) {
        return current;
    }

    return nullptr;
}

void MemTable::update_memory_usage(const Row& row) {
    memory_usage_ += row.size() + sizeof(SkipListNode);
}

}  // namespace clickhouse