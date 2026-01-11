cmake_minimum_required(VERSION 3.16)
project(ClickHouseMergeTree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add compile options for better debugging and warnings
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Include directories
include_directories(src)

# Source files
set(SOURCES
    src/row.cpp
    src/granule.cpp
    src/serialization.cpp
    src/sparse_index.cpp
    src/memtable.cpp
    src/part.cpp
    src/merger.cpp
    src/merge_tree.cpp
)

# Create library
add_library(clickhouse_mergetree STATIC ${SOURCES})

# Example executable
add_executable(demo examples/demo.cpp)
target_link_libraries(demo clickhouse_mergetree)

# Optional: Add threading support
find_package(Threads REQUIRED)
target_link_libraries(clickhouse_mergetree Threads::Threads)

# Create data directory if it doesn't exist
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/data)