// Copyright 2023 The titan-search Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include <rocksdb/options.h>
#include <memory>
#include <string>
#include <vector>

namespace titandb {
    constexpr const size_t KiB = 1024L;
    constexpr const size_t MiB = 1024L * KiB;
    constexpr const size_t GiB = 1024L * MiB;

    constexpr const char *kDefaultNamespace = "__namespace";

    struct CompactionCheckerRange {
    public:
        int start;
        int stop;

        bool Enabled() const { return start != -1 || stop != -1; }
    };

    struct Config {
    public:
        Config() = default;

        ~Config() = default;

        int max_db_size = 0;
        int max_io_mb = 0;
        int max_bitmap_to_string_mb = 16;
        std::string db_dir;
        std::string db_name;
        CompactionCheckerRange compaction_checker_range{-1, -1};
        int64_t force_compact_file_age;
        int force_compact_file_min_deleted_percentage;

        bool slot_id_encoded = false;
        bool cluster_enabled = false;

        struct RocksDB {
            int block_size;
            bool cache_index_and_filter_blocks;
            int metadata_block_cache_size;
            int subkey_block_cache_size;
            bool share_metadata_and_subkey_block_cache;
            int row_cache_size;
            int max_open_files;
            int write_buffer_size;
            int max_write_buffer_number;
            int max_background_compactions;
            int max_background_flushes;
            int max_sub_compactions;
            int stats_dump_period_sec;
            bool enable_pipelined_write;
            int64_t delayed_write_rate;
            int compaction_readahead_size;
            int target_file_size_base;
            int wal_ttl_seconds;
            int wal_size_limit_mb;
            int max_total_wal_size;
            int level0_slowdown_writes_trigger;
            int level0_stop_writes_trigger;
            int level0_file_num_compaction_trigger;
            int compression;
            bool disable_auto_compactions;
            bool enable_blob_files;
            int min_blob_size;
            int blob_file_size;
            bool enable_blob_garbage_collection;
            int blob_garbage_collection_age_cutoff;
            int max_bytes_for_level_base;
            int max_bytes_for_level_multiplier;
            bool level_compaction_dynamic_level_bytes;
            int max_background_jobs;
            bool rate_limiter_auto_tuned;

            struct WriteOptions {
                bool sync;
                bool disable_wal;
                bool no_slowdown;
                bool low_pri;
                bool memtable_insert_hint_per_batch;
            } write_options;

            struct ReadOptions {
                bool async_io;
            } read_options;
        } rocks_db;

    };
}  // namespace titandb
