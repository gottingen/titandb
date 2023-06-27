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

#include "titandb/db.h"

namespace titandb {

    turbo::ResultStatus<std::string> TitanDB::HGet(const std::string_view &key, const std::string_view &field) {
        std::string value;
        auto s = _db->Get(key, field, &value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return value;
    }

    turbo::ResultStatus<int64_t>
    TitanDB::HIncrBy(const std::string_view &key, const std::string_view &field, int64_t value) {
        int64_t ret;
        auto s = _db->IncrBy(key, field, value, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<double>
    TitanDB::HIncrByFloat(const std::string_view &key, const std::string_view &field, double value) {
        double ret;
        auto s = _db->IncrByFloat(key, field, value, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int>
    TitanDB::HSet(const std::string_view &key, const std::string_view &field, const std::string_view &value) {
        int ret;
        auto s = _db->Set(key, field, value, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::Status TitanDB::HMSet(const std::string_view &key, const std::vector<FieldValue> &kvs) {
        int ret;
        auto s = _db->MSet(key, kvs, false, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return turbo::OkStatus();
    }

    turbo::Status TitanDB::HSetNx(const std::string_view &key, const std::vector<FieldValue> &kvs) {
        int ret;
        auto s = _db->MSet(key, kvs, true, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return turbo::OkStatus();
    }

    turbo::ResultStatus<int> TitanDB::HDel(const std::string_view &key, const std::vector<std::string_view> &fields) {
        int ret;
        auto s = _db->Delete(key, fields, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<size_t> TitanDB::HStrlen(const std::string_view &key, const std::string_view &field) {
        std::string value;
        auto s = _db->Get(key, field, &value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return value.size();
    }

    turbo::ResultStatus<bool> TitanDB::HExists(const std::string_view &key, const std::string_view &field) {
        std::string value;
        auto s = _db->Get(key, field, &value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return !s.IsNotFound();
    }

    turbo::ResultStatus<size_t> TitanDB::HLen(const std::string_view &key) {
        uint32_t value;
        auto s = _db->Size(key, &value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return s.IsNotFound() ? 0ul : value;
    }

    turbo::ResultStatus<std::vector<turbo::ResultStatus<std::string>>>
    TitanDB::HMGet(const std::string_view &key, const std::vector<std::string_view> &fields) {
        std::vector<std::string> values;
        std::vector<rocksdb::Status> statuses;
        auto s = _db->MGet(key, fields, &values, &statuses);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        std::vector<turbo::ResultStatus<std::string>> results;
        for (size_t i = 0; i < statuses.size(); ++i) {
            if (!statuses[i].ok()) {
                results.emplace_back(turbo::NotFoundError(""));
            } else {
                results.emplace_back(std::move(values[i]));
            }
        }
        return results;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::HKeys(const std::string_view &key) {
        std::vector<FieldValue> fvs;
        auto s = _db->GetAll(key, &fvs, HashFetchType::kOnlyKey);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }

        std::vector<std::string> res;
        for (auto &&item: fvs) {
            res.emplace_back(std::move(item.field));
        }
        return res;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::HVals(const std::string_view &key) {
        std::vector<FieldValue> fvs;
        auto s = _db->GetAll(key, &fvs, HashFetchType::kOnlyValue);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }

        std::vector<std::string> res;
        for (auto &&item: fvs) {
            res.emplace_back(std::move(item.value));
        }
        return res;
    }

    turbo::ResultStatus<std::vector<FieldValue>> TitanDB::HGetAll(const std::string_view &key) {
        std::vector<FieldValue> fvs;
        auto s = _db->GetAll(key, &fvs, HashFetchType::kAll);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return fvs;
    }

    turbo::ResultStatus<FieldValueArray>
    TitanDB::HScan(const std::string_view &key, const std::string_view &cursor, const std::string_view &prefix,
                   uint64_t limits) {
        FieldValueArray fvs{{},{}};
        auto s = _db->Scan(key, cursor, limits, prefix, &fvs.fields, &fvs.values);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return fvs;
    }

    turbo::ResultStatus<std::vector<FieldValue>> TitanDB::HRangeByLex(const std::string_view &key, RangeLexSpec spec) {
        std::vector<FieldValue> results;
        auto s = _db->RangeByLex(key, spec, &results);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }
}  // namespace titandb

