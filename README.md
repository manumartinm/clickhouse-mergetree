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

## Usage

```cpp
#include "merge_tree.h"

// Create MergeTree instance
MergeTree engine("./data", 1000);  // 1000 rows per memtable flush

// Insert data
engine.insert("key1", "value1", 1234567890);
engine.insert("key2", "value2", 1234567891);

// Query data
auto results = engine.query("key1", "key2");
```

## File Structure

```
data/
├── part_001/
│   ├── primary.idx     # Sparse primary key index
│   ├── key.bin         # Key column data
│   ├── value.bin       # Value column data
│   └── timestamp.bin   # Timestamp column data
├── part_002/
│   └── ...
└── memtable.wal       # Write-ahead log
```

## Performance Characteristics

- **Insert Throughput**: ~14K rows/sec (single-threaded)
- **Query Performance**: ~316 rows/μs scanning speed
- **Memory Efficiency**: 1.3 MB for 50K rows with 50 parts
- **Storage Efficiency**: ~42 bytes per row overhead (before compression)
- **Index Overhead**: Sparse index scales linearly with part count

## Implementation Details

This implementation demonstrates the core concepts of ClickHouse MergeTree while keeping complexity manageable. It includes:

1. **Write Path**: Memtable → Flush to Part → Background Merge
2. **Read Path**: Sparse Index → Granule Loading → Column Scanning
3. **Storage Format**: Column files with granule-based organization
4. **Merge Strategy**: Simple size-based part selection

## Blog Post

See [`BLOG_POST.md`](./BLOG_POST.md) for a comprehensive deep-dive into the implementation, architectural decisions, performance analysis, and LSM-tree concepts.

## Project Statistics

- **Total Code**: ~2000 lines of C++
- **Core Implementation**: 1803 lines (src/ directory)
- **Demo/Examples**: 209 lines
- **Key Components**:
  - MergeTree engine: 385 lines
  - Part management: 358 lines
  - Merger logic: 271 lines
  - Serialization: 222 lines
  - MemTable: 230 lines
  - Other components: 337 lines

## Testing Results

```
Insert performance: 50000 rows in 3579 ms (13970.4 rows/sec)
Query performance: 5545 results in 17530 μs
Final stats:
  Parts: 50
  Total rows: 50000
  Memory usage: 1327 KB
  Disk usage: 2091 KB
```