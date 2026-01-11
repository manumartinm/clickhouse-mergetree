#include "merge_tree.h"
#include <iostream>
#include <chrono>
#include <random>
#include <cassert>

using namespace clickhouse;

void test_basic_operations() {
    std::cout << "=== Testing Basic Operations ===" << std::endl;

    MergeTreeConfig config;
    config.memtable_flush_threshold = 100;
    config.max_parts = 5;
    config.enable_background_merge = false;

    MergeTree engine("./data/test_basic", config);

    std::cout << "Inserting test data..." << std::endl;
    engine.insert("key1", "value1", 1000);
    engine.insert("key2", "value2", 2000);
    engine.insert("key3", "value3", 3000);
    engine.insert("key1", "updated_value1", 4000);

    std::cout << "Querying single key..." << std::endl;
    auto results = engine.query_key("key1");
    std::cout << "Found " << results.size() << " entries for key1" << std::endl;
    for (const auto& row : results) {
        std::cout << "  " << row.key << " -> " << row.value << " (ts: " << row.timestamp << ")" << std::endl;
    }

    std::cout << "Querying range..." << std::endl;
    auto range_results = engine.query("key1", "key3");
    std::cout << "Found " << range_results.size() << " entries in range [key1, key3]" << std::endl;

    engine.shutdown();
    std::cout << "Basic operations test completed successfully!" << std::endl << std::endl;
}

void test_memtable_flush() {
    std::cout << "=== Testing Memtable Flush ===" << std::endl;

    MergeTreeConfig config;
    config.memtable_flush_threshold = 10;
    config.enable_background_merge = false;

    MergeTree engine("./data/test_flush", config);

    std::cout << "Inserting data to trigger flush..." << std::endl;
    for (int i = 0; i < 25; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        engine.insert(key, value, i * 1000);
    }

    std::cout << "Parts after inserts: " << engine.part_count() << std::endl;
    std::cout << "Total rows: " << engine.total_rows() << std::endl;

    engine.flush_memtable();
    std::cout << "Parts after manual flush: " << engine.part_count() << std::endl;

    engine.shutdown();
    std::cout << "Memtable flush test completed successfully!" << std::endl << std::endl;
}

void test_merge_operations() {
    std::cout << "=== Testing Merge Operations ===" << std::endl;

    MergeTreeConfig config;
    config.memtable_flush_threshold = 20;
    config.max_parts = 3;
    config.enable_background_merge = false;

    MergeTree engine("./data/test_merge", config);

    std::cout << "Creating multiple parts..." << std::endl;
    for (int batch = 0; batch < 10; ++batch) {
        for (int i = 0; i < 25; ++i) {
            std::string key = "batch" + std::to_string(batch) + "_key" + std::to_string(i);
            std::string value = "value_" + std::to_string(batch) + "_" + std::to_string(i);
            engine.insert(key, value, batch * 1000 + i);
        }
    }

    std::cout << "Parts before merge: " << engine.part_count() << std::endl;
    std::cout << "Total rows before merge: " << engine.total_rows() << std::endl;

    engine.optimize();

    std::cout << "Parts after optimization: " << engine.part_count() << std::endl;
    std::cout << "Total rows after merge: " << engine.total_rows() << std::endl;

    auto results = engine.query("batch0", "batch2");
    std::cout << "Query results from merged data: " << results.size() << " rows" << std::endl;

    engine.shutdown();
    std::cout << "Merge operations test completed successfully!" << std::endl << std::endl;
}

void test_performance() {
    std::cout << "=== Performance Test ===" << std::endl;

    MergeTreeConfig config;
    config.memtable_flush_threshold = 1000;
    config.max_parts = 10;
    config.enable_background_merge = true;
    config.merge_interval_seconds = 5;

    MergeTree engine("./data/test_performance", config);

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(1, 10000);

    const int num_inserts = 50000;

    std::cout << "Inserting " << num_inserts << " rows..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_inserts; ++i) {
        std::string key = "key_" + std::to_string(dist(rng));
        std::string value = "value_" + std::to_string(i);
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        engine.insert(key, value, timestamp);

        if (i % 10000 == 0 && i > 0) {
            std::cout << "Inserted " << i << " rows, parts: " << engine.part_count() << std::endl;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Insert performance: " << num_inserts << " rows in " << duration.count()
              << " ms (" << (num_inserts * 1000.0 / duration.count()) << " rows/sec)" << std::endl;

    std::cout << "Final stats:" << std::endl;
    std::cout << "  Parts: " << engine.part_count() << std::endl;
    std::cout << "  Total rows: " << engine.total_rows() << std::endl;
    std::cout << "  Memory usage: " << (engine.memory_usage() / 1024) << " KB" << std::endl;
    std::cout << "  Disk usage: " << (engine.disk_usage() / 1024) << " KB" << std::endl;

    start = std::chrono::high_resolution_clock::now();
    auto query_results = engine.query("key_1000", "key_2000");
    end = std::chrono::high_resolution_clock::now();
    auto query_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << "Query performance: " << query_results.size() << " results in "
              << query_duration.count() << " μs" << std::endl;

    engine.shutdown();
    std::cout << "Performance test completed successfully!" << std::endl << std::endl;
}

void test_persistence() {
    std::cout << "=== Testing Persistence ===" << std::endl;

    const std::string data_path = "./data/test_persistence";

    {
        std::cout << "Creating engine and inserting data..." << std::endl;
        MergeTree engine(data_path);

        for (int i = 0; i < 100; ++i) {
            std::string key = "persistent_key" + std::to_string(i);
            std::string value = "persistent_value" + std::to_string(i);
            engine.insert(key, value, i * 1000);
        }

        engine.flush_memtable();
        std::cout << "Data written, parts: " << engine.part_count() << std::endl;
    }

    {
        std::cout << "Recreating engine and loading existing data..." << std::endl;
        MergeTree engine(data_path);

        std::cout << "Loaded parts: " << engine.part_count() << std::endl;
        std::cout << "Total rows: " << engine.total_rows() << std::endl;

        auto results = engine.query("persistent_key50", "persistent_key60");
        std::cout << "Query results from persistent data: " << results.size() << " rows" << std::endl;

        for (const auto& row : results) {
            std::cout << "  " << row.key << " -> " << row.value << std::endl;
        }
    }

    std::cout << "Persistence test completed successfully!" << std::endl << std::endl;
}

int main() {
    std::cout << "ClickHouse MergeTree Implementation Demo" << std::endl;
    std::cout << "=========================================" << std::endl << std::endl;

    try {
        test_basic_operations();
        test_memtable_flush();
        test_merge_operations();
        test_performance();
        test_persistence();

        std::cout << "All tests completed successfully!" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}