#pragma once

#include "part.h"
#include <vector>
#include <memory>
#include <queue>

namespace clickhouse {

struct MergeCandidate {
    std::vector<size_t> part_indices;
    size_t total_rows;
    size_t total_size;
    double score;

    MergeCandidate() : total_rows(0), total_size(0), score(0.0) {}
};

class MergeIterator {
private:
    struct RowWithSource {
        Row row;
        size_t part_index;
        size_t row_index;

        bool operator>(const RowWithSource& other) const {
            return row.key > other.row.key ||
                   (row.key == other.row.key && row.timestamp < other.row.timestamp);
        }
    };

    std::vector<std::unique_ptr<Part>> parts_;
    std::vector<RowVector> part_rows_;
    std::vector<size_t> current_indices_;
    std::priority_queue<RowWithSource, std::vector<RowWithSource>, std::greater<RowWithSource>> heap_;

public:
    explicit MergeIterator(std::vector<std::unique_ptr<Part>> parts);

    bool has_next() const;

    Row next();

    void advance_part(size_t part_index);

private:
    void initialize_heap();
};

class Merger {
private:
    std::string base_path_;
    size_t next_part_id_;

public:
    explicit Merger(const std::string& base_path);

    std::unique_ptr<Part> merge_parts(std::vector<std::unique_ptr<Part>> parts);

    std::vector<MergeCandidate> select_merge_candidates(
        const std::vector<std::unique_ptr<Part>>& parts,
        size_t max_candidates = 3) const;

    size_t get_next_part_id() const;

    void set_next_part_id(size_t id);

private:
    double calculate_merge_score(const std::vector<size_t>& part_indices,
                                const std::vector<std::unique_ptr<Part>>& parts) const;

    RowVector merge_rows(std::vector<std::unique_ptr<Part>> parts);
};

}  // namespace clickhouse