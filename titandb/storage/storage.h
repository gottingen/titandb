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

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/backup_engine.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <atomic>
#include <cinttypes>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>
#include <shared_mutex>

#include "titandb/config.h"
#include "titandb/storage/lock_manager.h"
#include "titandb/common/observer_or_unique.h"
#include "turbo/base/status.h"

const int kReplIdLength = 16;


namespace titandb {

    constexpr const char *kMetadataColumnFamilyName = "metadata";
    constexpr const char *kSubkeyColumnFamilyName = "default";


    class Storage {
    public:
        explicit Storage(Config *config);

        ~Storage();

        void SetWriteOptions(const Config::RocksDB::WriteOptions &config);

        void SetReadOptions(rocksdb::ReadOptions &read_options);

        turbo::Status Open(bool read_only = false);

        void CloseDB();

        void EmptyDB();

        rocksdb::Status CreateCheckPoint(const std::string& db_snapshot_path);

        rocksdb::BlockBasedTableOptions InitTableOptions();

        void SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options);

        rocksdb::Options InitRocksDBOptions();

        turbo::Status SetOptionForAllColumnFamilies(const std::string &key, const std::string &value);

        turbo::Status SetOption(const std::string &key, const std::string &value);

        turbo::Status SetDBOption(const std::string &key, const std::string &value);

        turbo::Status RestoreFromCheckpoint(const std::string &path);

        turbo::Status GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter);

        turbo::Status ReplicaApplyWriteBatch(std::string &&raw_batch);

        rocksdb::SequenceNumber LatestSeqNumber();

        rocksdb::Status Get(const rocksdb::ReadOptions &options, const rocksdb::Slice &key, std::string *value);

        rocksdb::Status Get(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                            const rocksdb::Slice &key, std::string *value);

        void MultiGet(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family, size_t num_keys,
                      const rocksdb::Slice *keys, rocksdb::PinnableSlice *values, rocksdb::Status *statuses);

        rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family);

        rocksdb::Iterator *NewIterator(const rocksdb::ReadOptions &options);

        rocksdb::Status Write(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);

        const rocksdb::WriteOptions &DefaultWriteOptions() { return write_opts_; }

        rocksdb::Status Delete(const rocksdb::WriteOptions &options, rocksdb::ColumnFamilyHandle *cf_handle,
                               const rocksdb::Slice &key);

        rocksdb::Status DeleteRange(const std::string &first_key, const std::string &last_key);


        bool WALHasNewData(rocksdb::SequenceNumber seq) { return seq <= LatestSeqNumber(); }

        turbo::Status InWALBoundary(rocksdb::SequenceNumber seq);

        rocksdb::Status Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end);

        rocksdb::DB *GetDB();

        bool IsClosing() const { return db_closing_; }

        std::string GetName() { return config_->db_name; }

        rocksdb::ColumnFamilyHandle *GetCFHandle(const std::string &name);

        std::vector<rocksdb::ColumnFamilyHandle *> *GetCFHandles() { return &cf_handles_; }

        LockManager *GetLockManager() { return &lock_mgr_; }

        uint64_t GetTotalSize(const std::string &ns = kDefaultNamespace);

        void CheckDBSizeLimit();

        void SetIORateLimit(int64_t max_io_mb);

        std::shared_lock<std::shared_mutex> ReadLockGuard();

        std::unique_lock<std::shared_mutex> WriteLockGuard();

        uint64_t GetFlushCount() { return flush_count_; }

        void IncrFlushCount(uint64_t n) { flush_count_.fetch_add(n); }

        uint64_t GetCompactionCount() { return compaction_count_; }

        void IncrCompactionCount(uint64_t n) { compaction_count_.fetch_add(n); }

        const Config *GetConfig() { return config_; }

        turbo::Status BeginTxn();

        turbo::Status CommitTxn();

        ObserverOrUniquePtr<rocksdb::WriteBatchBase> GetWriteBatchBase();

        Storage(const Storage &) = delete;

        Storage &operator=(const Storage &) = delete;

        bool ExistCheckpoint(const std::string&path);

        void SetDBInRetryableIOError(bool yes_or_no) { db_in_retryable_io_error_ = yes_or_no; }

        bool IsDBInRetryableIOError() { return db_in_retryable_io_error_; }

    private:
        turbo::Status TryCreateColumnFamilies(const rocksdb::Options &options);
        rocksdb::Status writeToDB(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
    private:
        rocksdb::DB *db_ = nullptr;
        time_t backup_creating_time_;
        rocksdb::BackupEngine *backup_ = nullptr;
        rocksdb::Env *env_;
        std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_;
        std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;
        std::shared_mutex checkpoint_mu_;
        Config *config_ = nullptr;
        std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
        LockManager lock_mgr_;
        bool db_size_limit_reached_ = false;
        std::atomic<uint64_t> flush_count_{0};
        std::atomic<uint64_t> compaction_count_{0};

        std::shared_mutex db_rw_lock_;
        bool db_closing_ = true;

        std::atomic<bool> db_in_retryable_io_error_{false};

        std::atomic<bool> is_txn_mode_ = false;
        // txn_write_batch_ is used as the global write batch for the transaction mode,
        // all writes will be grouped in this write batch when entering the transaction mode,
        // then write it at once when committing.
        //
        // Notice: the reason why we can use the global transaction? because the EXEC is an exclusive
        // command, so it won't have multi transactions to be executed at the same time.
        std::unique_ptr<rocksdb::WriteBatchWithIndex> txn_write_batch_;

        rocksdb::WriteOptions write_opts_ = rocksdb::WriteOptions();

    };

}  // namespace titandb
