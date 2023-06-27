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

    turbo::ResultStatus<std::string> TitanDB::Get(const std::string_view &key) {
        std::string value;
        auto s = _db->Get(key, &value);
        if (s.IsInvalidArgument()) {
            uint32_t max_btos_size = static_cast<uint32_t>(_config->max_bitmap_to_string_mb) * MiB;
            s = _db->GetString(key, max_btos_size, &value);
        }
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return value;
    }

    turbo::ResultStatus<std::string> TitanDB::GetEx(const std::string_view &key, int ttl) {
        std::string value;
        auto s = _db->GetEx(key, &value, ttl);
        if (s.IsInvalidArgument()) {
            uint32_t max_btos_size = static_cast<uint32_t>(_config->max_bitmap_to_string_mb) * MiB;
            s = _db->GetString(key, max_btos_size, &value);
        }
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return value;
    }

    turbo::ResultStatus<size_t> TitanDB::Strlen(const std::string_view &key) {
        std::string value;
        auto s = _db->Get(key, &value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return  value.size();
    }

    turbo::ResultStatus<std::string> TitanDB::GetSet(const std::string_view &key, const std::string_view &new_value) {
        std::string old_value;
        auto s = _db->GetSet(key, new_value, &old_value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return old_value;
    }

    turbo::ResultStatus<std::string> TitanDB::GetDel(const std::string_view &key) {
        std::string value;
        auto s = _db->GetDel(key, &value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return value;
    }

    turbo::ResultStatus<std::string> TitanDB::GetRange(const std::string_view &key, int start, int stop) {
        std::string value;
        auto s = _db->Get(key, &value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }

        if (start < 0) {
            start = static_cast<int>(value.size()) + start;
        }
        if (stop < 0) {
            stop = static_cast<int>(value.size()) + stop;
        }
        if (start < 0) {
            start = 0;
        }
        if (stop > static_cast<int>(value.size())) {
            stop = static_cast<int>(value.size());
        }

        if (start > stop) {
            return "";
        } else {
            return value.substr(start, stop - start + 1);
        }
    }

    turbo::ResultStatus<std::string> TitanDB::SubStr(const std::string_view &key, int start, int stop) {
        return GetRange(key, start, stop);
    }

    turbo::ResultStatus<int> TitanDB::SetRange(const std::string_view &key, size_t offset, const std::string_view &value) {
        int ret;
        auto s = _db->SetRange(key, offset, value, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    //TODO(Jeff) fix Redis strings MGet method to avoid memory copy
    std::vector<turbo::ResultStatus<std::string>> TitanDB::MGet(const std::vector<std::string_view> &keys) {
        std::vector<std::string> values;
        auto vs = _db->MGet(keys, &values);
        std::vector<turbo::ResultStatus<std::string>> rets;
        rets.reserve(keys.size());
        for(size_t i = 0; i < vs.size(); ++i) {
            auto *s = &vs[i];
            if (!s->ok() && !s->IsNotFound()) {
                 rets.emplace_back(turbo::Status(turbo::kUnavailable, s->ToString()));
            }
            if(s->IsNotFound()) {
                rets.emplace_back(turbo::NotFoundError(""));
            }
            rets.emplace_back(values[i]);
        }
        return rets;
    }

    turbo::ResultStatus<size_t> TitanDB::Append(const std::string_view &key, const std::string_view & value) {
        int ret;
        auto s = _db->Append(key, value, &ret);
        if (!s.ok()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        return ret;
    }

    turbo::Status TitanDB::Set(const std::string_view &key, const std::string_view & value) {
        auto s = _db->Set(key, value);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return turbo::OkStatus();
    }

    turbo::Status TitanDB::SetEx(const std::string_view &key ,const std::string_view & value, int ttl) {
        if(ttl <= 0) {
            return turbo::Status{turbo::kInvalidArgument, "ttl must greater than 0"};
        }
        auto s = _db->SetEX(key, value, ttl * 1000);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return turbo::OkStatus();
    }

    turbo::Status TitanDB::PSetEx(const std::string_view &key,const std::string_view & value, int64_t ttl) {
        if(ttl <= 0) {
            return turbo::Status{turbo::kInvalidArgument, "ttl must greater than 0"};
        }
        auto s = _db->SetEX(key, value, ttl * 1000);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return turbo::OkStatus();
    }

    turbo::ResultStatus<int> TitanDB::SetNx(const std::string_view &key, const std::string_view &value) {
        int ret;
        auto s = _db->SetNX(key, value, 0, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::MSetNx(const std::vector<StringPair> &kvs) {
        int ret;
        auto s = _db->MSetNX(kvs, 0, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::MSet(const std::vector<StringPair> &kvs) {
        int ret;
        auto s = _db->MSet(kvs);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int64_t> TitanDB::IncrBy(const std::string_view &key, int64_t value) {
        int64_t ret;
        auto s = _db->IncrBy(key, value, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<double> TitanDB::IncrByFloat(const std::string_view &key, double value) {
        double ret;
        auto s = _db->IncrByFloat(key, value, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int64_t> TitanDB::Incr(const std::string_view &key) {
        int64_t ret;
        auto s = _db->IncrBy(key, 1, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int64_t> TitanDB::DecrBy(const std::string_view &key, int64_t value) {
        int64_t ret;
        auto s = _db->IncrBy(key, -1 * value, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int64_t> TitanDB::Decr(const std::string_view &key) {
        int64_t ret;
        auto s = _db->IncrBy(key, -1, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::CAS(const std::string_view &key, const std::string_view &old_value, std::string_view &new_value, int ttl) {
        int ret;
        auto s = _db->CAS(key,old_value, new_value, ttl * 1000, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::CAD(const std::string_view &key, const std::string_view & value) {
        int ret;
        auto s = _db->CAD(key,value, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }
}  // namespace titandb
