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
#include "turbo/time/clock.h"

namespace titandb {

    turbo::ResultStatus<RedisType> TitanDB::Type(const std::string_view &key) {
        RedisType ret;
        auto s = _key_db->Type(key, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int64_t> TitanDB::TTL(const std::string_view &key) {
        int64_t ret;
        auto s = _key_db->TTL(key, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret / 1000;
    }

    turbo::ResultStatus<int64_t> TitanDB::PTTL(const std::string_view &key) {
        int64_t ret;
        auto s = _key_db->TTL(key, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::Object(const std::string_view &key) {
        std::vector<std::string> ret;
        auto s = _key_db->Dump(key, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::Exists(const std::vector<std::string_view> &keys) {
        int ret;
        auto s = _key_db->Exists(keys, &ret);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::Persist(const std::string_view &key) {
        int64_t ttl = 0;
        auto s = _key_db->TTL(key, &ttl);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        if (ttl == -1 || ttl == -2) {
            return 0;
        }
        s = _key_db->Expire(key, 0);
        if (!s.ok()) {
            return turbo::Status{turbo::kUnavailable, s.ToString()};
        }
        return 1;
    }

    turbo::ResultStatus<int> TitanDB::Expire(const std::string_view &key, int ttl) {
        auto s = _key_db->Expire(key, turbo::ToUnixMillis(turbo::Now()) + ttl * 1000);
        if (!s.ok()) {
            return 0;
        }
        return 1;
    }

    turbo::ResultStatus<int> TitanDB::PExpire(const std::string_view &key, int64_t ttl) {
        auto s = _key_db->Expire(key, turbo::ToUnixMillis(turbo::Now()) + ttl);
        if (!s.ok()) {
            return 0;
        }
        return 1;
    }

    turbo::ResultStatus<int> TitanDB::ExpireAt(const std::string_view &key, int ttl) {
        auto s = _key_db->Expire(key, ttl * 1000);
        if (!s.ok()) {
            return 0;
        }
        return 1;
    }

    turbo::ResultStatus<int> TitanDB::PExpireAt(const std::string_view &key, int64_t ttl) {
        auto s = _key_db->Expire(key, ttl * 1000);
        if (!s.ok()) {
            return 0;
        }
        return 1;
    }

    turbo::ResultStatus<int> TitanDB::Del(const std::string_view &key) {
        auto s = _key_db->Del(key);
        if (!s.ok()) {
            return 0;
        }
        return 1;
    }

    turbo::ResultStatus<int> TitanDB::Unlink(const std::string_view &key) {
        auto s = _key_db->Del(key);
        if (!s.ok()) {
            return 0;
        }
        return 1;
    }
}  // namespace titandb
