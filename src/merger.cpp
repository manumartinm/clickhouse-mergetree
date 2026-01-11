#include "merger.h"
#include <algorithm>
#include <stdexcept>

namespace clickhouse {

MergeIterator::MergeIterator(std::vector<std::unique_ptr<Part>> parts)
    : parts_(std::move(parts)) {

    part_rows_.resize(parts_.size());
    current_indices_.resize(parts_.size(), 0);

    for (size_t i = 0; i < parts_.size(); ++i) {
        part_rows_[i] = parts_[i]->get_all_rows();
    }

    initialize_heap();
}

bool MergeIterator::has_next() const {
    return !heap_.empty();
}

Row MergeIterator::next() {
    if (heap_.empty()) {
        throw std::runtime_error("No more rows to merge");
    }

    auto current = heap_.top();
    heap_.pop();

    advance_part(current.part_index);

    return current.row;
}

void MergeIterator::advance_part(size_t part_index) {
    current_indices_[part_index]++;

    if (current_indices_[part_index] < part_rows_[part_index].size()) {
        RowWithSource row_with_source;
        row_with_source.row = part_rows_[part_index][current_indices_[part_index]];
        row_with_source.part_index = part_index;
        row_with_source.row_index = current_indices_[part_index];
        heap_.push(row_with_source);
    }
}

void MergeIterator::initialize_heap() {
    for (size_t i = 0; i < parts_.size(); ++i) {
        if (!part_rows_[i].empty()) {
            RowWithSource row_with_source;
            row_with_source.row = part_rows_[i][0];
            row_with_source.part_index = i;
            row_with_source.row_index = 0;
            heap_.push(row_with_source);
        }
    }
}

Merger::Merger(const std::string& base_path) : base_path_(base_path), next_part_id_(1) {}

std::unique_ptr<Part> Merger::merge_parts(std::vector<std::unique_ptr<Part>> parts) {
    if (parts.empty()) {
        throw std::runtime_error("Cannot merge empty parts");
    }

    if (parts.size() == 1) {
        return std::move(parts[0]);
    }

    auto merged_rows = merge_rows(std::move(parts));

    if (merged_rows.empty()) {
        throw std::runtime_error("Merge resulted in empty rows");
    }

    auto merged_part = std::make_unique<Part>(get_next_part_id(), base_path_);
    merged_part->write_from_memtable_rows(merged_rows);

    return merged_part;
}

std::vector<MergeCandidate> Merger::select_merge_candidates(
    const std::vector<std::unique_ptr<Part>>& parts,
    size_t max_candidates) const {

    std::vector<MergeCandidate> candidates;

    if (parts.size() < 2) {
        return candidates;
    }

    for (size_t i = 0; i < parts.size() && candidates.size() < max_candidates; ++i) {
        for (size_t j = i + 1; j < parts.size() && candidates.size() < max_candidates; ++j) {
            MergeCandidate candidate;
            candidate.part_indices = {i, j};
            candidate.total_rows = parts[i]->metadata().row_count + parts[j]->metadata().row_count;
            candidate.total_size = parts[i]->disk_usage() + parts[j]->disk_usage();
            candidate.score = calculate_merge_score(candidate.part_indices, parts);

            if (candidate.score > 0) {
                candidates.push_back(candidate);
            }
        }
    }

    for (size_t i = 0; i < parts.size() - 2 && candidates.size() < max_candidates; ++i) {
        MergeCandidate candidate;
        candidate.part_indices = {i, i + 1, i + 2};
        candidate.total_rows = parts[i]->metadata().row_count +
                              parts[i + 1]->metadata().row_count +
                              parts[i + 2]->metadata().row_count;
        candidate.total_size = parts[i]->disk_usage() +
                              parts[i + 1]->disk_usage() +
                              parts[i + 2]->disk_usage();
        candidate.score = calculate_merge_score(candidate.part_indices, parts);

        if (candidate.score > 0) {
            candidates.push_back(candidate);
        }
    }

    std::sort(candidates.begin(), candidates.end(),
        [](const MergeCandidate& a, const MergeCandidate& b) {
            return a.score > b.score;
        });

    return candidates;
}

size_t Merger::get_next_part_id() const {
    return next_part_id_;
}

void Merger::set_next_part_id(size_t id) {
    next_part_id_ = id;
}

double Merger::calculate_merge_score(const std::vector<size_t>& part_indices,
                                    const std::vector<std::unique_ptr<Part>>& parts) const {
    if (part_indices.empty()) {
        return 0.0;
    }

    size_t total_rows = 0;
    size_t total_size = 0;
    size_t min_size = SIZE_MAX;
    size_t max_size = 0;

    for (size_t idx : part_indices) {
        if (idx >= parts.size()) {
            return 0.0;
        }

        size_t part_size = parts[idx]->disk_usage();
        total_rows += parts[idx]->metadata().row_count;
        total_size += part_size;
        min_size = std::min(min_size, part_size);
        max_size = std::max(max_size, part_size);
    }

    if (total_rows == 0 || total_size == 0) {
        return 0.0;
    }

    double size_ratio = static_cast<double>(min_size) / static_cast<double>(max_size);

    double parts_factor = 1.0 / part_indices.size();

    double size_factor = std::min(1.0, static_cast<double>(total_size) / (10 * 1024 * 1024));

    return size_ratio * parts_factor * size_factor * 100.0;
}

RowVector Merger::merge_rows(std::vector<std::unique_ptr<Part>> parts) {
    RowVector merged_rows;

    if (parts.empty()) {
        return merged_rows;
    }

    MergeIterator iterator(std::move(parts));

    while (iterator.has_next()) {
        Row current_row = iterator.next();

        if (merged_rows.empty() ||
            merged_rows.back().key != current_row.key ||
            merged_rows.back().timestamp != current_row.timestamp) {
            merged_rows.push_back(current_row);
        }
    }

    return merged_rows;
}

}  // namespace clickhouse