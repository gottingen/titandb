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

#include "titandb/storage/storage.h"
#include <fcntl.h>
#include "turbo/log/logging.h"
#include <rocksdb/convenience.h>
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/table_properties_collectors.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <random>

#include "titandb/storage/compact_filter.h"
#include "titandb/storage/event_listener.h"
#include "titandb/storage/redis_db.h"
#include "titandb/storage/redis_metadata.h"
#include "titandb/storage/table_properties_collector.h"
#include "turbo/times/clock.h"
#include "turbo/crypto/crc32c.h"
#include "titandb/common/unique_fd.h"
#include "turbo/format/str_format.h"

namespace titandb {

    constexpr const char *kReplicationIdKey = "replication_id_";

    const int64_t kIORateLimitMaxMb = 1024000;

    using rocksdb::Slice;

    Storage::Storage(Config *config)
            : backup_creating_time_(turbo::ToTimeT(turbo::Now())), env_(rocksdb::Env::Default()), config_(config),
              lock_mgr_(16) {
        Metadata::InitVersionCounter();
        SetWriteOptions(config->rocks_db.write_options);
    }

    Storage::~Storage() {
        DestroyBackup();
        CloseDB();
    }

    void Storage::CloseDB() {
        auto guard = WriteLockGuard();
        if (!db_) return;

        db_closing_ = true;
        db_->SyncWAL();
        rocksdb::CancelAllBackgroundWork(db_, true);
        for (auto handle: cf_handles_) db_->DestroyColumnFamilyHandle(handle);
        delete db_;
        db_ = nullptr;
    }

    void Storage::SetWriteOptions(const Config::RocksDB::WriteOptions &config) {
        write_opts_.sync = config.sync;
        write_opts_.disableWAL = config.disable_wal;
        write_opts_.no_slowdown = config.no_slowdown;
        write_opts_.low_pri = config.low_pri;
        write_opts_.memtable_insert_hint_per_batch = config.memtable_insert_hint_per_batch;
    }

    void Storage::SetReadOptions(rocksdb::ReadOptions &read_options) {
        read_options.fill_cache = false;
    }

    rocksdb::BlockBasedTableOptions Storage::InitTableOptions() {
        rocksdb::BlockBasedTableOptions table_options;
        table_options.format_version = 5;
        table_options.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.partition_filters = true;
        table_options.optimize_filters_for_memory = true;
        table_options.metadata_block_size = 4096;
        table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
        table_options.data_block_hash_table_util_ratio = 0.75;
        table_options.block_size = static_cast<size_t>(config_->rocks_db.block_size);
        return table_options;
    }

    void Storage::SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options) {
        cf_options->enable_blob_files = config_->rocks_db.enable_blob_files;
        cf_options->min_blob_size = config_->rocks_db.min_blob_size;
        cf_options->blob_file_size = config_->rocks_db.blob_file_size;
        cf_options->blob_compression_type = static_cast<rocksdb::CompressionType>(config_->rocks_db.compression);
        cf_options->enable_blob_garbage_collection = config_->rocks_db.enable_blob_garbage_collection;
        // Use 100.0 to force converting blob_garbage_collection_age_cutoff to double
        cf_options->blob_garbage_collection_age_cutoff = config_->rocks_db.blob_garbage_collection_age_cutoff / 100.0;
    }

    rocksdb::Options Storage::InitRocksDBOptions() {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;
        // options.IncreaseParallelism(2);
        // NOTE: the overhead of statistics is 5%-10%, so it should be configurable in prod env
        // See: https://github.com/facebook/rocksdb/wiki/Statistics
        options.statistics = rocksdb::CreateDBStatistics();
        options.stats_dump_period_sec = config_->rocks_db.stats_dump_period_sec;
        options.max_open_files = config_->rocks_db.max_open_files;
        options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
        options.max_subcompactions = static_cast<uint32_t>(config_->rocks_db.max_sub_compactions);
        options.max_background_flushes = config_->rocks_db.max_background_flushes;
        options.max_background_compactions = config_->rocks_db.max_background_compactions;
        options.max_write_buffer_number = config_->rocks_db.max_write_buffer_number;
        options.min_write_buffer_number_to_merge = 2;
        options.write_buffer_size = config_->rocks_db.write_buffer_size * MiB;
        options.num_levels = 7;
        options.compression_per_level.resize(options.num_levels);
        // only compress levels >= 2
        for (int i = 0; i < options.num_levels; ++i) {
            if (i < 2) {
                options.compression_per_level[i] = rocksdb::CompressionType::kNoCompression;
            } else {
                options.compression_per_level[i] = static_cast<rocksdb::CompressionType>(config_->rocks_db.compression);
            }
        }
        if (config_->rocks_db.row_cache_size) {
            options.row_cache = rocksdb::NewLRUCache(config_->rocks_db.row_cache_size * MiB);
        }
        options.enable_pipelined_write = config_->rocks_db.enable_pipelined_write;
        options.target_file_size_base = config_->rocks_db.target_file_size_base * MiB;
        options.max_manifest_file_size = 64 * MiB;
        options.max_log_file_size = 256 * MiB;
        options.keep_log_file_num = 12;
        options.WAL_ttl_seconds = static_cast<uint64_t>(config_->rocks_db.wal_ttl_seconds);
        options.WAL_size_limit_MB = static_cast<uint64_t>(config_->rocks_db.wal_size_limit_mb);
        options.max_total_wal_size = static_cast<uint64_t>(config_->rocks_db.max_total_wal_size * MiB);
        options.listeners.emplace_back(new EventListener(this));
        options.dump_malloc_stats = true;
        sst_file_manager_ = std::shared_ptr<rocksdb::SstFileManager>(
                rocksdb::NewSstFileManager(rocksdb::Env::Default()));
        options.sst_file_manager = sst_file_manager_;
        int64_t max_io_mb = kIORateLimitMaxMb;
        if (config_->max_io_mb > 0) max_io_mb = config_->max_io_mb;

        rate_limiter_ = std::shared_ptr<rocksdb::RateLimiter>(rocksdb::NewGenericRateLimiter(
                max_io_mb * static_cast<int64_t>(MiB), 100 * 1000, /* default */
                10,                                                /* default */
                rocksdb::RateLimiter::Mode::kWritesOnly, config_->rocks_db.rate_limiter_auto_tuned));

        options.rate_limiter = rate_limiter_;
        options.delayed_write_rate = static_cast<uint64_t>(config_->rocks_db.delayed_write_rate);
        options.compaction_readahead_size = static_cast<size_t>(config_->rocks_db.compaction_readahead_size);
        options.level0_slowdown_writes_trigger = config_->rocks_db.level0_slowdown_writes_trigger;
        options.level0_stop_writes_trigger = config_->rocks_db.level0_stop_writes_trigger;
        options.level0_file_num_compaction_trigger = config_->rocks_db.level0_file_num_compaction_trigger;
        options.max_bytes_for_level_base = config_->rocks_db.max_bytes_for_level_base;
        options.max_bytes_for_level_multiplier = config_->rocks_db.max_bytes_for_level_multiplier;
        options.level_compaction_dynamic_level_bytes = config_->rocks_db.level_compaction_dynamic_level_bytes;
        options.max_background_jobs = config_->rocks_db.max_background_jobs;

        return options;
    }

    turbo::Status Storage::SetOptionForAllColumnFamilies(const std::string &key, const std::string &value) {
        for (auto &cf_handle: cf_handles_) {
            auto s = db_->SetOptions(cf_handle, {{key, value}});
            if (!s.ok()) return turbo::UnavailableError("");
        }
        return turbo::OkStatus();
    }

    turbo::Status Storage::SetOption(const std::string &key, const std::string &value) {
        auto s = db_->SetOptions({{key, value}});
        if (!s.ok()) return turbo::UnavailableError("");
        return turbo::OkStatus();
    }

    turbo::Status Storage::SetDBOption(const std::string &key, const std::string &value) {
        auto s = db_->SetDBOptions({{key, value}});
        if (!s.ok()) return turbo::UnavailableError(s.ToString());
        return turbo::OkStatus();
    }

    turbo::Status Storage::CreateColumnFamilies(const rocksdb::Options &options) {
        rocksdb::DB *tmp_db = nullptr;
        rocksdb::ColumnFamilyOptions cf_options(options);
        rocksdb::Status s = rocksdb::DB::Open(options, config_->db_dir, &tmp_db);
        if (s.ok()) {
            std::vector<std::string> cf_names = {kMetadataColumnFamilyName};
            std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
            s = tmp_db->CreateColumnFamilies(cf_options, cf_names, &cf_handles);
            if (!s.ok()) {
                delete tmp_db;
                return turbo::UnavailableError(s.ToString());
            }

            for (auto handle: cf_handles) tmp_db->DestroyColumnFamilyHandle(handle);
            tmp_db->Close();
            delete tmp_db;
        }

        if (!s.ok()) {
            // We try to create column families by opening the database without column families.
            // If it's ok means we didn't create column families (cannot open without column families if created).
            // When goes wrong, we need to check whether it's caused by column families NOT being opened or not.
            // If the status message contains `Column families not opened` means that we have created the column
            // families, let's ignore the error.
            std::string not_opened_prefix = "Column families not opened";
            if (s.IsInvalidArgument() && s.ToString().find(not_opened_prefix) != std::string::npos) {
                return turbo::OkStatus();
            }

            return turbo::UnavailableError(s.ToString());
        }

        return turbo::OkStatus();
    }

    turbo::Status Storage::Open(bool read_only) {
        auto guard = WriteLockGuard();
        db_closing_ = false;

        bool cache_index_and_filter_blocks = config_->rocks_db.cache_index_and_filter_blocks;
        size_t metadata_block_cache_size = config_->rocks_db.metadata_block_cache_size * MiB;
        size_t subkey_block_cache_size = config_->rocks_db.subkey_block_cache_size * MiB;

        rocksdb::Options options = InitRocksDBOptions();
        if (auto s = CreateColumnFamilies(options); !s.ok()) {
            return s;
        }

        std::shared_ptr<rocksdb::Cache> shared_block_cache;
        if (config_->rocks_db.share_metadata_and_subkey_block_cache) {
            size_t shared_block_cache_size = metadata_block_cache_size + subkey_block_cache_size;
            shared_block_cache = rocksdb::NewLRUCache(shared_block_cache_size, -1, false, 0.75);
        }

        rocksdb::BlockBasedTableOptions metadata_table_opts = InitTableOptions();
        metadata_table_opts.block_cache =
                shared_block_cache ? shared_block_cache : rocksdb::NewLRUCache(metadata_block_cache_size, -1, false,
                                                                               0.75);
        metadata_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
        metadata_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
        metadata_table_opts.cache_index_and_filter_blocks_with_high_priority = true;

        rocksdb::ColumnFamilyOptions metadata_opts(options);
        metadata_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(metadata_table_opts));
        metadata_opts.compaction_filter_factory = std::make_shared<MetadataFilterFactory>(this);
        metadata_opts.disable_auto_compactions = config_->rocks_db.disable_auto_compactions;
        // Enable whole key bloom filter in memtable
        metadata_opts.memtable_whole_key_filtering = true;
        metadata_opts.memtable_prefix_bloom_size_ratio = 0.1;
        metadata_opts.table_properties_collector_factories.emplace_back(
                NewCompactOnExpiredTableCollectorFactory(kMetadataColumnFamilyName, 0.3));
        SetBlobDB(&metadata_opts);

        rocksdb::BlockBasedTableOptions subkey_table_opts = InitTableOptions();
        subkey_table_opts.block_cache =
                shared_block_cache ? shared_block_cache : rocksdb::NewLRUCache(subkey_block_cache_size, -1, false,
                                                                               0.75);
        subkey_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
        subkey_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
        subkey_table_opts.cache_index_and_filter_blocks_with_high_priority = true;
        rocksdb::ColumnFamilyOptions subkey_opts(options);
        subkey_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(subkey_table_opts));
        subkey_opts.compaction_filter_factory = std::make_shared<SubKeyFilterFactory>(this);
        subkey_opts.disable_auto_compactions = config_->rocks_db.disable_auto_compactions;
        subkey_opts.table_properties_collector_factories.emplace_back(
                NewCompactOnExpiredTableCollectorFactory(kSubkeyColumnFamilyName, 0.3));
        SetBlobDB(&subkey_opts);

        std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
        // Caution: don't change the order of column family, or the handle will be mismatched
        column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, subkey_opts);
        column_families.emplace_back(kMetadataColumnFamilyName, metadata_opts);

        std::vector<std::string> old_column_families;
        auto s = rocksdb::DB::ListColumnFamilies(options, config_->db_dir, &old_column_families);
        if (!s.ok()) return turbo::UnavailableError(s.ToString());

        auto start = std::chrono::high_resolution_clock::now();
        if (read_only) {
            s = rocksdb::DB::OpenForReadOnly(options, config_->db_dir, column_families, &cf_handles_, &db_);
        } else {
            s = rocksdb::DB::Open(options, config_->db_dir, column_families, &cf_handles_, &db_);
        }
        auto end = std::chrono::high_resolution_clock::now();
        int64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        if (!s.ok()) {
            TLOG_INFO("[storage] Failed to load the data from disk: {} ms");
            return turbo::UnavailableError(s.ToString());
        }

        TLOG_INFO("[storage] Success to load the data from disk: {} ms", duration);
        return turbo::OkStatus();
    }

    turbo::Status Storage::CreateBackup() {
        TLOG_INFO("[storage] Start to create new backup");
        std::lock_guard<std::mutex> lg(config_->backup_mu);
        std::string task_backup_dir = config_->backup_dir;

        std::string tmpdir = task_backup_dir + ".tmp";
        // Maybe there is a dirty tmp checkpoint, try to clean it
        rocksdb::DestroyDB(tmpdir, rocksdb::Options());

        // 1) Create checkpoint of rocksdb for backup
        rocksdb::Checkpoint *checkpoint = nullptr;
        rocksdb::Status s = rocksdb::Checkpoint::Create(db_, &checkpoint);
        if (!s.ok()) {
            TLOG_WARN("Failed to create checkpoint object for backup. Error: {}", s.ToString());
            return turbo::UnavailableError(s.ToString());
        }

        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
        s = checkpoint->CreateCheckpoint(tmpdir, config_->rocks_db.write_buffer_size * MiB);
        if (!s.ok()) {
            TLOG_WARN("Failed to create checkpoint (snapshot) for backup. Error: {}", s.ToString());
            return turbo::UnavailableError(s.ToString());
        }

        // 2) Rename tmp backup to real backup dir
        if (s = rocksdb::DestroyDB(task_backup_dir, rocksdb::Options()); !s.ok()) {
            TLOG_WARN("[storage] Failed to clean old backup. Error: {}", s.ToString());
            return turbo::UnavailableError(s.ToString());
        }

        if (s = env_->RenameFile(tmpdir, task_backup_dir); !s.ok()) {
            TLOG_WARN("[storage] Failed to rename tmp backup. Error: {}", s.ToString());
            // Just try best effort
            if (s = rocksdb::DestroyDB(tmpdir, rocksdb::Options()); !s.ok()) {
                TLOG_WARN("[storage] Failed to clean tmp backup. Error: {}", s.ToString());
            }

            return turbo::UnavailableError(s.ToString());
        }

        // 'backup_mu_' can guarantee 'backup_creating_time_' is thread-safe
        backup_creating_time_ = static_cast<time_t>(turbo::ToTimeT(turbo::Now()));

        TLOG_INFO("[storage] Success to create new backup");
        return turbo::OkStatus();
    }

    void Storage::DestroyBackup() {
        if (!backup_) {
            return;
        }
        backup_->StopBackup();
        delete backup_;
        backup_ = nullptr;
    }

    turbo::Status Storage::RestoreFromBackup() {
        // TODO(@ruoshan): assert role to be slave
        // We must reopen the backup engine every time, as the files is changed
        rocksdb::BackupEngineOptions bk_option(config_->backup_sync_dir);
        auto s = rocksdb::BackupEngine::Open(db_->GetEnv(), bk_option, &backup_);
        if (!s.ok()) return turbo::UnavailableError(s.ToString());

        s = backup_->RestoreDBFromLatestBackup(config_->db_dir, config_->db_dir);
        if (!s.ok()) {
            TLOG_ERROR("[storage] Failed to restore database from the latest backup. Error: {}", s.ToString());
        } else {
            TLOG_INFO("[storage] RedisDB was restored from the latest backup");
        }

        // Reopen DB （should always try to reopen db even if restore failed, replication SST file CRC check may use it）
        auto s2 = Open();
        if (!s2.ok()) {
            TLOG_ERROR("[storage] Failed to reopen the database. Error: {}", s2.message());
            return turbo::UnavailableError(s2.ToString());
        }

        // Clean up backup engine
        backup_->PurgeOldBackups(0);
        DestroyBackup();

        return s.ok() ? turbo::OkStatus() : turbo::UnavailableError(s.ToString());
    }

    turbo::Status Storage::RestoreFromCheckpoint() {
        std::string checkpoint_dir = config_->sync_checkpoint_dir;
        std::string tmp_dir = config_->db_dir + ".tmp";

        // Clean old backups and checkpoints because server will work on the new db
        PurgeOldBackups(0, 0);
        rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());

        // Maybe there is no database directory
        auto s = env_->CreateDirIfMissing(config_->db_dir);
        if (!s.ok()) {
            return turbo::UnavailableError(
                    turbo::Format("Failed to create database directory '{}'. Error: {}", config_->db_dir,
                                  s.ToString()));
        }

        // Rename database directory to tmp, so we can restore if replica fails to load the checkpoint from master.
        // But only try best effort to make data safe
        s = env_->RenameFile(config_->db_dir, tmp_dir);
        if (!s.ok()) {
            if (auto s1 = Open(); !s1.ok()) {
                TLOG_ERROR("[storage] Failed to reopen database. Error: {}", s1.message());
            }
            return turbo::UnavailableError(
                    turbo::Format("Failed to rename database directory '{}' to '{}'. Error: {}", config_->db_dir,
                                  tmp_dir, s.ToString()));
        }

        // Rename checkpoint directory to database directory
        if (s = env_->RenameFile(checkpoint_dir, config_->db_dir); !s.ok()) {
            env_->RenameFile(tmp_dir, config_->db_dir);
            if (auto s1 = Open(); !s1.ok()) {
                TLOG_ERROR("[storage] Failed to reopen database. Error: {}", s1.message());
            }
            return turbo::UnavailableError(
                    turbo::Format("Failed to rename checkpoint directory '{}' to '{}'. Error: {}", checkpoint_dir,
                                  config_->db_dir, s.ToString()));
        }

        // Open the new database, restore if replica fails to open
        auto s2 = Open();
        if (!s2.ok()) {
            TLOG_WARN("[storage] Failed to open master checkpoint. Error: {}", s2.message());
            rocksdb::DestroyDB(config_->db_dir, rocksdb::Options());
            env_->RenameFile(tmp_dir, config_->db_dir);
            if (auto s1 = Open(); !s1.ok()) {
                TLOG_ERROR("[storage] Failed to reopen database. Error: {}", s1.message());
            }
            return turbo::UnavailableError(turbo::Format("Failed to open master checkpoint. Error: {}", s2.message()));
        }

        // Destroy the origin database
        if (s = rocksdb::DestroyDB(tmp_dir, rocksdb::Options()); !s.ok()) {
            TLOG_WARN("[storage] Failed to destroy the origin database at '{}'. Error:{}", tmp_dir, s.ToString());
        }
        return turbo::OkStatus();
    }

    void Storage::EmptyDB() {
        // Clean old backups and checkpoints
        PurgeOldBackups(0, 0);
        rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());

        auto s = rocksdb::DestroyDB(config_->db_dir, rocksdb::Options());
        if (!s.ok()) {
            TLOG_ERROR("[storage] Failed to destroy database. Error: {}", s.ToString());
        }
    }

    void Storage::PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
        time_t now = turbo::ToTimeT(turbo::Now());
        std::lock_guard<std::mutex> lg(config_->backup_mu);
        std::string task_backup_dir = config_->backup_dir;

        // Return if there is no backup
        auto s = env_->FileExists(task_backup_dir);
        if (!s.ok()) return;

        // No backup is needed to keep or the backup is expired, we will clean it.
        bool backup_expired = (backup_max_keep_hours != 0 &&
                               backup_creating_time_ + backup_max_keep_hours * 3600 < now);
        if (num_backups_to_keep == 0 || backup_expired) {
            s = rocksdb::DestroyDB(task_backup_dir, rocksdb::Options());
            if (s.ok()) {
                TLOG_INFO("[storage] Succeeded cleaning old backup that was created at {}", backup_creating_time_);
            } else {
                TLOG_INFO("[storage] Failed cleaning old backup that was created at {}. Error: {}",
                          backup_creating_time_, s.ToString());
            }
        }
    }

    turbo::Status
    Storage::GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
        auto s = db_->GetUpdatesSince(seq, iter);
        if (!s.ok()) return turbo::UnavailableError(s.ToString());

        if (!(*iter)->Valid()) return turbo::UnavailableError("iterator is not valid");

        return turbo::OkStatus();
    }

    rocksdb::SequenceNumber Storage::LatestSeqNumber() { return db_->GetLatestSequenceNumber(); }

    rocksdb::Status Storage::Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key, std::string *value) {
        return Get(options, db_->DefaultColumnFamily(), key, value);
    }

    rocksdb::Status Storage::Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                                 const rocksdb::Slice &key, std::string *value) {
        if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
            return txn_write_batch_->GetFromBatchAndDB(db_, options, column_family, key, value);
        }
        return db_->Get(options, column_family, key, value);
    }

    rocksdb::Iterator *Storage::NewIterator(const rocksdb::ReadOptions &options) {
        return NewIterator(options, db_->DefaultColumnFamily());
    }

    rocksdb::Iterator *Storage::NewIterator(const rocksdb::ReadOptions &options,
                                            rocksdb::ColumnFamilyHandle *column_family) {
        auto iter = db_->NewIterator(options, column_family);
        if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
            return txn_write_batch_->NewIteratorWithBase(column_family, iter, &options);
        }
        return iter;
    }

    void Storage::MultiGet(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                           const size_t num_keys, const rocksdb::Slice *keys, rocksdb::PinnableSlice *values,
                           rocksdb::Status *statuses) {
        if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
            txn_write_batch_->MultiGetFromBatchAndDB(db_, options, column_family, num_keys, keys, values, statuses,
                                                     false);
        } else {
            db_->MultiGet(options, column_family, num_keys, keys, values, statuses, false);
        }
    }

    rocksdb::Status Storage::Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates) {
        if (is_txn_mode_) {
            // The batch won't be flushed until the transaction was committed or rollback
            return rocksdb::Status::OK();
        }
        return writeToDB(options, updates);
    }

    rocksdb::Status Storage::writeToDB(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates) {
        if (db_size_limit_reached_) {
            return rocksdb::Status::SpaceLimit();
        }

        return db_->Write(options, updates);
    }

    rocksdb::Status Storage::Delete(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle,
                                    const rocksdb::Slice &key) {
        auto batch = GetWriteBatchBase();
        batch->Delete(cf_handle, key);
        return Write(options, batch->GetWriteBatch());
    }

    rocksdb::Status Storage::DeleteRange(const std::string &first_key, const std::string &last_key) {
        auto batch = GetWriteBatchBase();
        rocksdb::ColumnFamilyHandle *cf_handle = GetCFHandle(kMetadataColumnFamilyName);
        auto s = batch->DeleteRange(cf_handle, first_key, last_key);
        if (!s.ok()) {
            return s;
        }

        s = batch->Delete(cf_handle, last_key);
        if (!s.ok()) {
            return s;
        }

        return Write(write_opts_, batch->GetWriteBatch());
    }

    turbo::Status Storage::ReplicaApplyWriteBatch(std::string &&raw_batch) {
        if (db_size_limit_reached_) {
            return turbo::UnavailableError("reach space limit");
        }

        auto batch = rocksdb::WriteBatch(std::move(raw_batch));
        auto s = db_->Write(write_opts_, &batch);
        if (!s.ok()) {
            return turbo::UnavailableError(s.ToString());
        }

        return turbo::OkStatus();
    }

    rocksdb::ColumnFamilyHandle *Storage::GetCFHandle(const std::string &name) {
        if (name == kMetadataColumnFamilyName) {
            return cf_handles_[1];
        }
        return cf_handles_[0];
    }

    rocksdb::Status Storage::Compact(const Slice *begin, const Slice *end) {
        rocksdb::CompactRangeOptions compact_opts;
        compact_opts.change_level = true;
        for (const auto &cf_handle: cf_handles_) {
            rocksdb::Status s = db_->CompactRange(compact_opts, cf_handle, begin, end);
            if (!s.ok()) return s;
        }
        return rocksdb::Status::OK();
    }

    uint64_t Storage::GetTotalSize(const std::string &ns) {
        if (ns == kDefaultNamespace) {
            return sst_file_manager_->GetTotalSize();
        }

        std::string prefix, begin_key, end_key;
        ComposeNamespaceKey(ns, "", &prefix, false);

        RedisDB db(this, ns);
        uint64_t size = 0, total_size = 0;
        rocksdb::DB::SizeApproximationFlags include_both =
                static_cast<rocksdb::DB::SizeApproximationFlags>(rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES |
                                                                 rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES);

        for (auto cf_handle: cf_handles_) {
            auto s = db.FindKeyRangeWithPrefix(prefix, std::string(), &begin_key, &end_key, cf_handle);
            if (!s.ok()) continue;

            rocksdb::Range r(begin_key, end_key);
            db_->GetApproximateSizes(cf_handle, &r, 1, &size, (uint8_t)include_both);
            total_size += size;
        }

        return total_size;
    }

    void Storage::CheckDBSizeLimit() {
        bool limit_reached = false;
        if (config_->max_db_size > 0) {
            limit_reached = GetTotalSize() >= config_->max_db_size * GiB;
        }

        if (db_size_limit_reached_ == limit_reached) {
            return;
        }

        db_size_limit_reached_ = limit_reached;
        if (db_size_limit_reached_) {
            TLOG_WARN("[storage] ENABLE db_size limit {} GB. Switch kvrocks to read-only mode.", config_->max_db_size);
        } else {
            TLOG_WARN("[storage] DISABLE db_size limit. Switch kvrocks to read-write mode.");
        }
    }

    void Storage::SetIORateLimit(int64_t max_io_mb) {
        if (max_io_mb == 0) {
            max_io_mb = kIORateLimitMaxMb;
        }
        rate_limiter_->SetBytesPerSecond(max_io_mb * static_cast<int64_t>(MiB));
    }

    rocksdb::DB *Storage::GetDB() { return db_; }

    turbo::Status Storage::BeginTxn() {
        if (is_txn_mode_) {
            return turbo::UnavailableError("cannot begin a new transaction while already in transaction mode");
        }
        // The EXEC command is exclusive and shouldn't have multi transaction at the same time,
        // so it's fine to reset the global write batch without any lock.
        is_txn_mode_ = true;
        txn_write_batch_ = std::make_unique<rocksdb::WriteBatchWithIndex>();
        return turbo::OkStatus();
    }

    turbo::Status Storage::CommitTxn() {
        if (!is_txn_mode_) {
            return turbo::UnavailableError("cannot commit while not in transaction mode");
        }

        auto s = writeToDB(write_opts_, txn_write_batch_->GetWriteBatch());

        is_txn_mode_ = false;
        txn_write_batch_ = nullptr;
        if (s.ok()) {
            return turbo::OkStatus();
        }
        return turbo::UnavailableError(s.ToString());
    }

    ObserverOrUniquePtr<rocksdb::WriteBatchBase> Storage::GetWriteBatchBase() {
        if (is_txn_mode_) {
            return ObserverOrUniquePtr<rocksdb::WriteBatchBase>(txn_write_batch_.get(), ObserverOrUnique::Observer);
        }
        return ObserverOrUniquePtr<rocksdb::WriteBatchBase>(new rocksdb::WriteBatch(), ObserverOrUnique::Unique);
    }


    std::shared_lock<std::shared_mutex> Storage::ReadLockGuard() { return std::shared_lock(db_rw_lock_); }

    std::unique_lock<std::shared_mutex> Storage::WriteLockGuard() { return std::unique_lock(db_rw_lock_); }


    bool Storage::ExistCheckpoint() {
        std::lock_guard<std::mutex> lg(checkpoint_mu_);
        return env_->FileExists(config_->checkpoint_dir).ok();
    }

    bool Storage::ExistSyncCheckpoint() { return env_->FileExists(config_->sync_checkpoint_dir).ok(); }

    turbo::Status Storage::InWALBoundary(rocksdb::SequenceNumber seq) {
        std::unique_ptr<rocksdb::TransactionLogIterator> iter;
        auto s = GetWALIter(seq, &iter);
        if (!s.ok()) return s;
        auto wal_seq = iter->GetBatch().sequence;
        if (seq < wal_seq) {
            return turbo::UnavailableError(
                    turbo::Format("checkpoint seq: {} is smaller than the WAL seq: {}", seq, wal_seq));
        }
        return turbo::OkStatus();
    }


    turbo::Status MkdirRecursively(rocksdb::Env *env, const std::string &dir) {
        if (env->CreateDirIfMissing(dir).ok()) return turbo::OkStatus();

        std::string parent;
        for (auto pos = dir.find('/', 1); pos != std::string::npos; pos = dir.find('/', pos + 1)) {
            parent = dir.substr(0, pos);
            if (auto s = env->CreateDirIfMissing(parent); !s.ok()) {
                TLOG_ERROR("[storage] Failed to create directory '{}' recursively. Error: {}", parent, s.ToString());
                return turbo::UnavailableError("");
            }
        }

        if (env->CreateDirIfMissing(dir).ok()) return turbo::OkStatus();

        return turbo::UnavailableError("");
    }
}  // namespace titandb
