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

#include "titandb/redis_db.h"
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include "turbo/strings/numbers.h"
#include "titandb/storage/redis_metadata.h"
#include "turbo/times/clock.h"

namespace titandb {

    std::vector<rocksdb::Status> RedisDB::getRawValues(const std::vector<Slice> &keys,
                                                           std::vector<std::string> *raw_values) {
        raw_values->clear();

        rocksdb::ReadOptions read_options;
        LatestSnapShot ss(storage_);
        read_options.snapshot = ss.GetSnapShot();
        raw_values->resize(keys.size());
        std::vector<rocksdb::Status> statuses(keys.size());
        std::vector<rocksdb::PinnableSlice> pin_values(keys.size());
        storage_->MultiGet(read_options, metadata_cf_handle_, keys.size(), keys.data(), pin_values.data(),
                           statuses.data());
        for (size_t i = 0; i < keys.size(); i++) {
            if (!statuses[i].ok()) continue;
            (*raw_values)[i].assign(pin_values[i].data(), pin_values[i].size());
            Metadata metadata(kRedisNone, false);
            metadata.Decode((*raw_values)[i]);
            if (metadata.Expired()) {
                (*raw_values)[i].clear();
                statuses[i] = rocksdb::Status::NotFound(kErrMsgKeyExpired);
                continue;
            }
            if (metadata.Type() != kRedisString && metadata.size > 0) {
                (*raw_values)[i].clear();
                statuses[i] = rocksdb::Status::InvalidArgument(kErrMsgWrongType);
                continue;
            }
        }
        return statuses;
    }

    rocksdb::Status RedisDB::getRawValue(const std::string_view &ns_key, std::string *raw_value) {
        raw_value->clear();

        rocksdb::ReadOptions read_options;
        LatestSnapShot ss(storage_);
        read_options.snapshot = ss.GetSnapShot();
        rocksdb::Status s = storage_->Get(read_options, metadata_cf_handle_, ns_key, raw_value);
        if (!s.ok()) return s;

        Metadata metadata(kRedisNone, false);
        metadata.Decode(*raw_value);
        if (metadata.Expired()) {
            raw_value->clear();
            return rocksdb::Status::NotFound(kErrMsgKeyExpired);
        }
        if (metadata.Type() != kRedisString && metadata.size > 0) {
            return rocksdb::Status::InvalidArgument(kErrMsgWrongType);
        }
        return rocksdb::Status::OK();
    }

    rocksdb::Status RedisDB::getValue(const std::string_view &ns_key, std::string *value) {
        value->clear();

        std::string raw_value;
        auto s = getRawValue(ns_key, &raw_value);
        if (!s.ok()) return s;
        size_t offset = Metadata::GetOffsetAfterExpire(raw_value[0]);
        *value = raw_value.substr(offset);
        return rocksdb::Status::OK();
    }

    std::vector<rocksdb::Status>
    RedisDB::getValues(const std::vector<Slice> &ns_keys, std::vector<std::string> *values) {
        auto statuses = getRawValues(ns_keys, values);
        for (size_t i = 0; i < ns_keys.size(); i++) {
            if (!statuses[i].ok()) continue;
            size_t offset = Metadata::GetOffsetAfterExpire((*values)[i][0]);
            (*values)[i] = (*values)[i].substr(offset, (*values)[i].size() - offset);
        }
        return statuses;
    }

    rocksdb::Status RedisDB::updateRawValue(const Slice &ns_key, const std::string_view &raw_value) {
        auto batch = storage_->GetWriteBatchBase();
        WriteBatchLogData log_data(kRedisString);
        batch->PutLogData(log_data.Encode());
        batch->Put(metadata_cf_handle_, ns_key, raw_value);
        return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    }

    rocksdb::Status RedisDB::Append(const std::string_view &user_key, const std::string_view &value, int *ret) {
        *ret = 0;
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        std::string raw_value;
        rocksdb::Status s = getRawValue(ns_key, &raw_value);
        if (!s.ok() && !s.IsNotFound()) return s;
        if (s.IsNotFound()) {
            Metadata metadata(kRedisString, false);
            metadata.Encode(&raw_value);
        }
        raw_value.append(value);
        *ret = static_cast<int>(raw_value.size() - Metadata::GetOffsetAfterExpire(raw_value[0]));
        return updateRawValue(ns_key, raw_value);
    }

    std::vector<rocksdb::Status> RedisDB::MGet(const std::vector<std::string_view> &keys, std::vector<std::string> *values) {
        std::vector<std::string> ns_keys;
        ns_keys.reserve(keys.size());
        for (const auto &key: keys) {
            std::string ns_key;
            AppendNamespacePrefix(key, &ns_key);
            ns_keys.emplace_back(ns_key);
        }
        std::vector<Slice> slice_keys;
        slice_keys.reserve(ns_keys.size());
        for (const auto &ns_key: ns_keys) {
            slice_keys.emplace_back(ns_key);
        }
        return getValues(slice_keys, values);
    }

    rocksdb::Status RedisDB::Get(const std::string_view &user_key, std::string *value) {
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);
        return getValue(ns_key, value);
    }

    rocksdb::Status RedisDB::GetEx(const std::string_view &user_key, std::string *value, uint64_t ttl) {
        uint64_t expire = 0;
        if (ttl > 0) {
            uint64_t now = turbo::ToUnixMillis(turbo::Now());
            expire = now + ttl;
        }
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        rocksdb::Status s = getValue(ns_key, value);
        if (!s.ok() && s.IsNotFound()) return s;

        std::string raw_data;
        Metadata metadata(kRedisString, false);
        metadata.expire = expire;
        metadata.Encode(&raw_data);
        raw_data.append(value->data(), value->size());
        auto batch = storage_->GetWriteBatchBase();
        WriteBatchLogData log_data(kRedisString);
        batch->PutLogData(log_data.Encode());
        batch->Put(metadata_cf_handle_, ns_key, raw_data);
        s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
        if (!s.ok()) return s;
        return rocksdb::Status::OK();
    }

    rocksdb::Status
    RedisDB::GetSet(const std::string_view &user_key, const std::string_view &new_value, std::string *old_value) {
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        rocksdb::Status s = getValue(ns_key, old_value);
        if (!s.ok() && !s.IsNotFound()) return s;

        std::string raw_value;
        Metadata metadata(kRedisString, false);
        metadata.Encode(&raw_value);
        raw_value.append(new_value.data(), new_value.size());
        auto write_status = updateRawValue(ns_key, raw_value);
        // prev status was used to tell whether old value was empty or not
        return !write_status.ok() ? write_status : s;
    }

    rocksdb::Status RedisDB::GetDel(const std::string_view &user_key, std::string *value) {
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        rocksdb::Status s = getValue(ns_key, value);
        if (!s.ok()) return s;

        return storage_->Delete(storage_->DefaultWriteOptions(), metadata_cf_handle_, ns_key);
    }

    rocksdb::Status RedisDB::Set(const std::string_view &user_key, const std::string_view &value) {
        std::vector<StringPair> pairs{StringPair{user_key, value}};
        return MSet(pairs, 0);
    }

    rocksdb::Status RedisDB::SetEX(const std::string_view &user_key, const std::string_view &value, uint64_t ttl) {
        std::vector<StringPair> pairs{StringPair{user_key, value}};
        return MSet(pairs, ttl);
    }

    rocksdb::Status RedisDB::SetNX(const std::string_view &user_key, const std::string_view &value, uint64_t ttl, int *ret) {
        std::vector<StringPair> pairs{StringPair{user_key, value}};
        return MSetNX(pairs, ttl, ret);
    }

    rocksdb::Status RedisDB::SetXX(const std::string_view &user_key, const std::string_view &value, uint64_t ttl, int *ret) {
        *ret = 0;
        int exists = 0;
        uint64_t expire = 0;
        if (ttl > 0) {
            uint64_t now = turbo::ToUnixMillis(turbo::Now());
            expire = now + ttl;
        }

        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);
        LockGuard guard(storage_->GetLockManager(), ns_key);
        Exists({user_key}, &exists);
        if (exists != 1) return rocksdb::Status::OK();

        *ret = 1;
        std::string raw_value;
        Metadata metadata(kRedisString, false);
        metadata.expire = expire;
        metadata.Encode(&raw_value);
        raw_value.append(value);
        return updateRawValue(ns_key, raw_value);
    }

    rocksdb::Status
    RedisDB::SetRange(const std::string_view &user_key, size_t offset, const std::string_view &value, int *ret) {
        std::string ns_key;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        std::string raw_value;
        rocksdb::Status s = getRawValue(ns_key, &raw_value);
        if (!s.ok() && !s.IsNotFound()) return s;

        if (s.IsNotFound()) {
            // Return 0 directly instead of storing an empty key when set nothing on a non-existing string.
            if (value.empty()) {
                *ret = 0;
                return rocksdb::Status::OK();
            }

            Metadata metadata(kRedisString, false);
            metadata.Encode(&raw_value);
        }

        size_t size = raw_value.size();
        size_t header_offset = Metadata::GetOffsetAfterExpire(raw_value[0]);
        offset += header_offset;
        if (offset > size) {
            // padding the value with zero byte while offset is longer than value size
            size_t paddings = offset - size;
            raw_value.append(paddings, '\0');
        }
        if (offset + value.size() >= size) {
            raw_value = raw_value.substr(0, offset);
            raw_value.append(value.data(), value.size());
        } else {
            for (size_t i = 0; i < value.size(); i++) {
                raw_value[offset + i] = value[i];
            }
        }
        *ret = static_cast<int>(raw_value.size() - header_offset);
        return updateRawValue(ns_key, raw_value);
    }

    rocksdb::Status RedisDB::IncrBy(const std::string_view &user_key, int64_t increment, int64_t *ret) {
        std::string ns_key, value;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        std::string raw_value;
        rocksdb::Status s = getRawValue(ns_key, &raw_value);
        if (!s.ok() && !s.IsNotFound()) return s;
        if (s.IsNotFound()) {
            Metadata metadata(kRedisString, false);
            metadata.Encode(&raw_value);
        }

        size_t offset = Metadata::GetOffsetAfterExpire(raw_value[0]);
        value = raw_value.substr(offset);
        int64_t n = 0;
        if (!value.empty()) {
            if (!turbo::SimpleAtoi(value, &n)) {
                return rocksdb::Status::InvalidArgument("value is not an integer or out of range");
            }
            if (isspace(value[0])) {
                return rocksdb::Status::InvalidArgument("value is not an integer");
            }
        }
        if ((increment < 0 && n <= 0 && increment < (LLONG_MIN - n)) ||
            (increment > 0 && n >= 0 && increment > (LLONG_MAX - n))) {
            return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
        }
        n += increment;
        *ret = n;

        raw_value = raw_value.substr(0, offset);
        raw_value.append(std::to_string(n));
        return updateRawValue(ns_key, raw_value);
    }

    rocksdb::Status RedisDB::IncrByFloat(const std::string_view &user_key, double increment, double *ret) {
        std::string ns_key, value;
        AppendNamespacePrefix(user_key, &ns_key);
        LockGuard guard(storage_->GetLockManager(), ns_key);
        std::string raw_value;
        rocksdb::Status s = getRawValue(ns_key, &raw_value);
        if (!s.ok() && !s.IsNotFound()) return s;

        if (s.IsNotFound()) {
            Metadata metadata(kRedisString, false);
            metadata.Encode(&raw_value);
        }
        size_t offset = Metadata::GetOffsetAfterExpire(raw_value[0]);
        value = raw_value.substr(offset);
        double n = 0;
        if (!value.empty()) {
            if (!turbo::SimpleAtod(value, &n) || isspace(value[0])) {
                return rocksdb::Status::InvalidArgument("value is not a number");
            }
        }

        n += increment;
        if (std::isinf(n) || std::isnan(n)) {
            return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
        }
        *ret = n;

        raw_value = raw_value.substr(0, offset);
        raw_value.append(std::to_string(n));
        return updateRawValue(ns_key, raw_value);
    }

    rocksdb::Status RedisDB::MSet(const std::vector<StringPair> &pairs, uint64_t ttl) {
        uint64_t expire = 0;
        if (ttl > 0) {
            uint64_t now = turbo::ToUnixMillis(turbo::Now());
            expire = now + ttl;
        }

        // Data race, key string maybe overwrite by other key while didn't lock the key here,
        // to improve the set performance
        std::string ns_key;
        for (const auto &pair: pairs) {
            std::string bytes;
            Metadata metadata(kRedisString, false);
            metadata.expire = expire;
            metadata.Encode(&bytes);
            bytes.append(pair.value.data(), pair.value.size());
            auto batch = storage_->GetWriteBatchBase();
            WriteBatchLogData log_data(kRedisString);
            batch->PutLogData(log_data.Encode());
            AppendNamespacePrefix(pair.key, &ns_key);
            batch->Put(metadata_cf_handle_, ns_key, bytes);
            LockGuard guard(storage_->GetLockManager(), ns_key);
            auto s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
            if (!s.ok()) return s;
        }
        return rocksdb::Status::OK();
    }

    rocksdb::Status RedisDB::MSetNX(const std::vector<StringPair> &pairs, uint64_t ttl, int *ret) {
        *ret = 0;

        uint64_t expire = 0;
        if (ttl > 0) {
            uint64_t now = turbo::ToUnixMillis(turbo::Now());
            expire = now + ttl;
        }

        int exists = 0;
        std::vector<std::string_view> keys;
        keys.reserve(pairs.size());
        for (StringPair pair: pairs) {
            keys.emplace_back(pair.key);
        }
        if (Exists(keys, &exists).ok() && exists > 0) {
            return rocksdb::Status::OK();
        }

        std::string ns_key;
        for (StringPair pair: pairs) {
            AppendNamespacePrefix(pair.key, &ns_key);
            LockGuard guard(storage_->GetLockManager(), ns_key);
            if (Exists({pair.key}, &exists).ok() && exists == 1) {
                return rocksdb::Status::OK();
            }
            std::string bytes;
            Metadata metadata(kRedisString, false);
            metadata.expire = expire;
            metadata.Encode(&bytes);
            bytes.append(pair.value.data(), pair.value.size());
            auto batch = storage_->GetWriteBatchBase();
            WriteBatchLogData log_data(kRedisString);
            batch->PutLogData(log_data.Encode());
            batch->Put(metadata_cf_handle_, ns_key, bytes);
            auto s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
            if (!s.ok()) return s;
        }
        *ret = 1;
        return rocksdb::Status::OK();
    }

    // Change the value of user_key to a new_value if the current value of the key matches old_value.
    // ret will be:
    //  1 if the operation is successful
    //  -1 if the user_key does not exist
    //  0 if the operation fails
    rocksdb::Status
    RedisDB::CAS(const std::string_view &user_key, const std::string_view &old_value, const std::string_view &new_value,
                     uint64_t ttl, int *ret) {
        *ret = 0;

        std::string ns_key, current_value;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        rocksdb::Status s = getValue(ns_key, &current_value);

        if (!s.ok() && !s.IsNotFound()) {
            return s;
        }

        if (s.IsNotFound()) {
            *ret = -1;
            return rocksdb::Status::OK();
        }

        if (old_value == current_value) {
            std::string raw_value;
            uint64_t expire = 0;
            Metadata metadata(kRedisString, false);
            if (ttl > 0) {
                uint64_t now = turbo::ToUnixMillis(turbo::Now());
                expire = now + ttl;
            }
            metadata.expire = expire;
            metadata.Encode(&raw_value);
            raw_value.append(new_value);
            auto write_status = updateRawValue(ns_key, raw_value);
            if (!write_status.ok()) {
                return write_status;
            }
            *ret = 1;
        }

        return rocksdb::Status::OK();
    }

    // Delete a specified user_key if the current value of the user_key matches a specified value.
    // For ret, same as CAS.
    rocksdb::Status RedisDB::CAD(const std::string_view &user_key, const std::string_view &value, int *ret) {
        *ret = 0;

        std::string ns_key, current_value;
        AppendNamespacePrefix(user_key, &ns_key);

        LockGuard guard(storage_->GetLockManager(), ns_key);
        rocksdb::Status s = getValue(ns_key, &current_value);

        if (!s.ok() && !s.IsNotFound()) {
            return s;
        }

        if (s.IsNotFound()) {
            *ret = -1;
            return rocksdb::Status::OK();
        }

        if (value == current_value) {
            auto delete_status = storage_->Delete(storage_->DefaultWriteOptions(),
                                                  storage_->GetCFHandle(kMetadataColumnFamilyName), ns_key);
            if (!delete_status.ok()) {
                return delete_status;
            }
            *ret = 1;
        }

        return rocksdb::Status::OK();
    }

}  // namespace titandb
