# Minimal ClickHouse MergeTree Implementation

A minimal but functional implementation of ClickHouse's MergeTree storage engine in C++, demonstrating core LSM-tree concepts and columnar storage.

**🚀 Fully functional with ~2000 lines of code**

## Features

- **Columnar Storage**: Data stored in column-oriented format for efficient analytics
- **LSM-Tree Architecture**: Write-optimized with background merging
- **Sparse Indexing**: Primary key index with granule-level entries
- **Memory Management**: Skip list-based memtable with configurable flush thresholds
- **Background Merging**: Automatic part consolidation for read optimization

## Architecture

The implementation consists of several key components:

- **Row/Granule**: Basic data structures for rows and 8192-row blocks
- **Memtable**: In-memory buffer using skip list for fast insertions
- **Part**: Immutable sorted data files on disk
- **Sparse Index**: Primary key index for efficient range scans
- **Merger**: Background process for part consolidation
- **MergeTree**: Main engine interface

## Building

```bash
mkdir build && cd build
cmake ..
make
```

## Blog Post

See [the blog post in my web](https://manumartinm.dev/blog/clickhouse-mergetree) for a comprehensive deep-dive into the implementation, architectural decisions, performance analysis, and LSM-tree concepts.