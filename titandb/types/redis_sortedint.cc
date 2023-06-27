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

#include "titandb/types/redis_sortedint.h"

#include <iostream>
#include <limits>

#include "titandb/storage/db_util.h"
#include "turbo/strings/numbers.h"

namespace titandb {

    rocksdb::Status RedisSortedInt::GetMetadata(const Slice &ns_key, SortedintMetadata *metadata) {
        return RedisDB::GetMetadata(kRedisSortedint, ns_key, metadata);
    }

    rocksdb::Status RedisSortedInt::Add(const Slice &user_key, const std::vector<uint64_t> &ids, int *ret) {
        *ret = 0;

        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        SortedintMetadata metadata;
        rocksdb::Status s = GetMetadata(ns_key, &metadata);
        if (!s.ok() && !s.IsNotFound()) return s;

        std::string value;
        auto batch = storage_->GetWriteBatchBase();
        WriteBatchLogData log_data(kRedisSortedint);
        batch->PutLogData(log_data.Encode());
        std::string sub_key;
        for (const auto id: ids) {
            std::string id_buf;
            PutFixed64(&id_buf, id);
            InternalKey(ns_key, id_buf, metadata.version).Encode(&sub_key);
            s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value);
            if (s.ok()) continue;
            batch->Put(sub_key, Slice());
            *ret += 1;
        }
        if (*ret > 0) {
            metadata.size += *ret;
            std::string bytes;
            metadata.Encode(&bytes);
            batch->Put(metadata_cf_handle_, ns_key, bytes);
        }
        return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    }

    rocksdb::Status RedisSortedInt::Remove(const Slice &user_key, const std::vector<uint64_t> &ids, int *ret) {
        *ret = 0;

        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        SortedintMetadata metadata(false);
        rocksdb::Status s = GetMetadata(ns_key, &metadata);
        if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

        std::string value, sub_key;
        auto batch = storage_->GetWriteBatchBase();
        WriteBatchLogData log_data(kRedisSortedint);
        batch->PutLogData(log_data.Encode());
        for (const auto id: ids) {
            std::string id_buf;
            PutFixed64(&id_buf, id);
            InternalKey(ns_key, id_buf, metadata.version).Encode(&sub_key);
            s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value);
            if (!s.ok()) continue;
            batch->Delete(sub_key);
            *ret += 1;
        }
        if (*ret == 0) return rocksdb::Status::OK();
        metadata.size -= *ret;
        std::string bytes;
        metadata.Encode(&bytes);
        batch->Put(metadata_cf_handle_, ns_key, bytes);
        return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    }

    rocksdb::Status RedisSortedInt::Card(const Slice &user_key, int *ret) {
        *ret = 0;
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        SortedintMetadata metadata(false);
        rocksdb::Status s = GetMetadata(ns_key, &metadata);
        if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
        *ret = static_cast<int>(metadata.size);
        return rocksdb::Status::OK();
    }

    rocksdb::Status RedisSortedInt::Range(const Slice &user_key, uint64_t cursor_id, uint64_t offset, uint64_t limit,
                                     bool reversed, std::vector<uint64_t> *ids) {
        ids->clear();

        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        SortedintMetadata metadata(false);
        rocksdb::Status s = GetMetadata(ns_key, &metadata);
        if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

        std::string prefix, next_version_prefix, start_key, start_buf;
        uint64_t start_id = cursor_id;
        if (reversed && cursor_id == 0) {
            start_id = std::numeric_limits<uint64_t>::max();
        }
        PutFixed64(&start_buf, start_id);
        InternalKey(ns_key, start_buf, metadata.version).Encode(&start_key);
        InternalKey(ns_key, "", metadata.version).Encode(&prefix);
        InternalKey(ns_key, "", metadata.version + 1).Encode(&next_version_prefix);

        rocksdb::ReadOptions read_options;
        LatestSnapShot ss(storage_);
        read_options.snapshot = ss.GetSnapShot();
        rocksdb::Slice upper_bound(next_version_prefix);
        read_options.iterate_upper_bound = &upper_bound;
        rocksdb::Slice lower_bound(prefix);
        read_options.iterate_lower_bound = &lower_bound;
        storage_->SetReadOptions(read_options);

        uint64_t id = 0, pos = 0;
        auto iter = UniqueIterator(storage_, read_options);
        for (!reversed ? iter->Seek(start_key) : iter->SeekForPrev(start_key);
             iter->Valid() && iter->key().starts_with(prefix); !reversed ? iter->Next() : iter->Prev()) {
            InternalKey ikey(iter->key());
            Slice sub_key = ikey.GetSubKey();
            GetFixed64(&sub_key, &id);
            if (id == cursor_id || pos++ < offset) continue;
            ids->emplace_back(id);
            if (limit > 0 && ids->size() >= limit) break;
        }
        return rocksdb::Status::OK();
    }

    rocksdb::Status RedisSortedInt::RangeByValue(const Slice &user_key, SortedintRangeSpec spec, std::vector<uint64_t> *ids,
                                            int *size) {
        if (size) *size = 0;
        if (ids) ids->clear();

        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        SortedintMetadata metadata(false);
        rocksdb::Status s = GetMetadata(ns_key, &metadata);
        if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

        std::string start_buf, start_key, prefix_key, next_version_prefix_key;
        PutFixed64(&start_buf, spec.reversed ? spec.max : spec.min);
        InternalKey(ns_key, start_buf, metadata.version).Encode(&start_key);
        InternalKey(ns_key, "", metadata.version).Encode(&prefix_key);
        InternalKey(ns_key, "", metadata.version + 1).Encode(&next_version_prefix_key);

        rocksdb::ReadOptions read_options;
        LatestSnapShot ss(storage_);
        read_options.snapshot = ss.GetSnapShot();
        rocksdb::Slice upper_bound(next_version_prefix_key);
        read_options.iterate_upper_bound = &upper_bound;
        rocksdb::Slice lower_bound(prefix_key);
        read_options.iterate_lower_bound = &lower_bound;
        storage_->SetReadOptions(read_options);

        int pos = 0;
        auto iter = UniqueIterator(storage_, read_options);
        if (!spec.reversed) {
            iter->Seek(start_key);
        } else {
            iter->SeekForPrev(start_key);
        }

        uint64_t id = 0;
        for (; iter->Valid() && iter->key().starts_with(prefix_key); !spec.reversed ? iter->Next() : iter->Prev()) {
            InternalKey ikey(iter->key());
            Slice sub_key = ikey.GetSubKey();
            GetFixed64(&sub_key, &id);
            if (spec.reversed) {
                if ((spec.minex && id == spec.min) || id < spec.min) break;
                if ((spec.maxex && id == spec.max) || id > spec.max) continue;
            } else {
                if ((spec.minex && id == spec.min) || id < spec.min) continue;
                if ((spec.maxex && id == spec.max) || id > spec.max) break;
            }
            if (spec.offset >= 0 && pos++ < spec.offset) continue;
            if (ids) ids->emplace_back(id);
            if (size) *size += 1;
            if (spec.count > 0 && ids && ids->size() >= static_cast<unsigned>(spec.count)) break;
        }
        return rocksdb::Status::OK();
    }

    rocksdb::Status
    RedisSortedInt::MExist(const Slice &user_key, const std::vector<uint64_t> &ids, std::vector<int> *exists) {
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        SortedintMetadata metadata(false);
        rocksdb::Status s = GetMetadata(ns_key, &metadata);
        if (!s.ok()) return s;

        LatestSnapShot ss(storage_);
        rocksdb::ReadOptions read_options;
        read_options.snapshot = ss.GetSnapShot();
        std::string sub_key, value;
        for (const auto id: ids) {
            std::string id_buf;
            PutFixed64(&id_buf, id);
            InternalKey(ns_key, id_buf, metadata.version).Encode(&sub_key);
            s = storage_->Get(read_options, sub_key, &value);
            if (!s.ok() && !s.IsNotFound()) return s;
            if (s.IsNotFound()) {
                exists->emplace_back(0);
            } else {
                exists->emplace_back(1);
            }
        }
        return rocksdb::Status::OK();
    }

    turbo::Status RedisSortedInt::ParseRangeSpec(const std::string &min, const std::string &max, SortedintRangeSpec *spec) {
        if (min == "+inf" || max == "-inf") {
            return turbo::InternalError("");
        }

        if (min == "-inf") {
            spec->min = std::numeric_limits<uint64_t>::lowest();
        } else {
            const char *sptr = min.data();
            if (!min.empty() && min[0] == '(') {
                spec->minex = true;
                sptr++;
            }
            if (!turbo::SimpleAtoi(sptr, &spec->min)) {
                return turbo::InternalError("");
            }
        }

        if (max == "+inf") {
            spec->max = std::numeric_limits<uint64_t>::max();
        } else {
            const char *sptr = max.data();
            if (!max.empty() && max[0] == '(') {
                spec->maxex = true;
                sptr++;
            }
            if (!turbo::SimpleAtoi(sptr, &spec->max)) {
                return turbo::InternalError("");
            }
        }
        return turbo::OkStatus();
    }

}  // namespace titandb
