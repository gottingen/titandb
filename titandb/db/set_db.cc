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

    turbo::ResultStatus<int> TitanDB::SAdd(const std::string_view &key, const std::vector<std::string_view> &elems) {
        int ret = 0;
        auto s = _set_db->Add(key, elems, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::SRem(const std::string_view &key, const std::vector<std::string_view> &elems) {
        int ret = 0;
        auto s = _set_db->Remove(key, elems, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::SCard(const std::string_view &key) {
        int ret = 0;
        auto s = _set_db->Card(key, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::SMembers(const std::string_view &key) {
        std::vector<std::string> ret;
        auto s = _set_db->Members(key, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<bool> TitanDB::SisMembers(const std::string_view &key, const std::string_view &value) {
        int ret;
        auto s = _set_db->IsMember(key, value, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret != 0;
    }

    turbo::ResultStatus<std::vector<int>> TitanDB::SmisMembers(const std::string_view &key, const std::vector<std::string_view> & elems) {
        std::vector<int> rets;
        auto s = _set_db->MIsMember(key, elems, &rets);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return rets;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::SPop(const std::string_view &key, int count) {
        std::vector<std::string> rets;
        auto s = _set_db->Take(key, &rets, count, true);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return rets;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::SRandMember(const std::string_view &key, int count) {
        std::vector<std::string> rets;
        auto s = _set_db->Take(key, &rets, count, false);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return rets;
    }

    turbo::ResultStatus<int> TitanDB::SMove(const std::string_view &src, const std::string_view &dst, const std::string_view &member) {
        int ret;
        auto s = _set_db->Move(src, dst, member, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::SDiff(const std::vector<std::string_view> &keys) {
        std::vector<std::string> results;
        auto s = _set_db->Diff(keys, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::SUnion(const std::vector<std::string_view> &keys) {
        std::vector<std::string> results;
        auto s = _set_db->Union(keys, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::SInter(const std::vector<std::string_view> &keys) {
        std::vector<std::string> results;
        auto s = _set_db->Inter(keys, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<int> TitanDB::SDiffStore(const std::string_view &dst, const std::vector<std::string_view> &keys) {
        int results;
        auto s = _set_db->DiffStore(dst, keys, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<int> TitanDB::SUnionStore(const std::string_view &dst, const std::vector<std::string_view> &keys) {
        int results;
        auto s = _set_db->UnionStore(dst, keys, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<int> TitanDB::SInterStore(const std::string_view &dst,  const std::vector<std::string_view> &keys) {
        int results;
        auto s = _set_db->InterStore(dst, keys, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::SScan(const std::string_view &user_key, const std::string_view &cursor, uint64_t limit, const std::string &member_prefix) {
        std::vector<std::string> results;
        auto s = _set_db->Scan(user_key, cursor,limit, member_prefix, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }
}  // namespace titandb
