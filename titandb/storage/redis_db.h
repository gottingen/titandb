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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "titandb/storage/redis_metadata.h"
#include "titandb/storage/storage.h"

namespace titandb {

    class RedisDB {
    public:
        static constexpr uint64_t RANDOM_KEY_SCAN_LIMIT = 60;

        explicit RedisDB(titandb::Storage *storage, std::string ns = "");

        void Refresh();
        rocksdb::Status GetMetadata(RedisType type, const Slice &ns_key, Metadata *metadata);

        rocksdb::Status GetRawMetadata(const Slice &ns_key, std::string *bytes);

        rocksdb::Status GetRawMetadataByUserKey(const Slice &user_key, std::string *bytes);

        rocksdb::Status Expire(const Slice &user_key, uint64_t timestamp);

        rocksdb::Status Del(const Slice &user_key);

        rocksdb::Status Exists(const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status TTL(const Slice &user_key, int64_t *ttl);

        rocksdb::Status Type(const Slice &user_key, RedisType *type);

        rocksdb::Status Dump(const Slice &user_key, std::vector<std::string> *infos);

        rocksdb::Status FlushDB();

        rocksdb::Status FlushAll();

        void GetKeyNumStats(const std::string &prefix, KeyNumStats *stats);

        void Keys(const std::string &prefix, std::vector<std::string> *keys = nullptr, KeyNumStats *stats = nullptr);

        rocksdb::Status Scan(const Slice &cursor, uint64_t limit, const Slice &prefix,
                             std::vector<std::string> *keys, std::string *end_cursor = nullptr);

        rocksdb::Status RandomKey(const std::string &cursor, std::string *key);

        void AppendNamespacePrefix(const Slice &user_key, std::string *output);

        rocksdb::Status
        FindKeyRangeWithPrefix(const std::string &prefix, const std::string &prefix_end, std::string *begin,
                               std::string *end, rocksdb::ColumnFamilyHandle *cf_handle = nullptr);

    protected:
        titandb::Storage *storage_;
        rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
        std::string namespace_;

        friend class LatestSnapShot;

        class LatestSnapShot {
        public:
            explicit LatestSnapShot(titandb::Storage *storage)
                    : storage_(storage), snapshot_(storage_->GetDB()->GetSnapshot()) {}

            ~LatestSnapShot() { storage_->GetDB()->ReleaseSnapshot(snapshot_); }

            const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }

            LatestSnapShot(const LatestSnapShot &) = delete;

            LatestSnapShot &operator=(const LatestSnapShot &) = delete;

        private:
            titandb::Storage *storage_ = nullptr;
            const rocksdb::Snapshot *snapshot_ = nullptr;
        };
    };

    class SubKeyScanner : public titandb::RedisDB {
    public:
        explicit SubKeyScanner(titandb::Storage *storage, const std::string &ns) : RedisDB(storage, ns) {}

        rocksdb::Status Scan(RedisType type, const Slice &user_key, const Slice &cursor, uint64_t limit,
                             const Slice &subkey_prefix, std::vector<std::string> *keys,
                             std::vector<std::string> *values = nullptr);
    };

    class WriteBatchLogData {
    public:
        WriteBatchLogData() = default;

        explicit WriteBatchLogData(RedisType type) : type_(type) {}

        explicit WriteBatchLogData(RedisType type, std::vector<std::string> &&args) : type_(type),
                                                                                      args_(std::move(args)) {}

        RedisType GetRedisType();

        std::vector<std::string> *GetArguments();

        std::string Encode();

        turbo::Status Decode(const rocksdb::Slice &blob);

    private:
        RedisType type_ = kRedisNone;
        std::vector<std::string> args_;
    };

}  // namespace titandb
