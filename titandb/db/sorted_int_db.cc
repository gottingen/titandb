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

    turbo::ResultStatus<int> TitanDB::SIAdd(const std::string_view &key, const std::vector<uint64_t> &ids) {
        int ret;
        auto s = _sorted_int_db->Add(key,ids, &ret);
        if(!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }
    turbo::ResultStatus<int> TitanDB::SIRem(const std::string_view &key, const std::vector<uint64_t> &ids) {
        int ret;
        auto s = _sorted_int_db->Remove(key,ids, &ret);
        if(!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }
    turbo::ResultStatus<int> TitanDB::SICard(const std::string_view &key) {
        int ret;
        auto s = _sorted_int_db->Card(key, &ret);
        if(!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<int>> TitanDB::SIExists(const std::string_view &key, const std::vector<uint64_t> &ids) {
        std::vector<int> ret;
        auto s = _sorted_int_db->MExist(key, ids, &ret);
        if(!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<uint64_t>> TitanDB::SIRange(const Slice &key, uint64_t cursor_id, uint64_t page, uint64_t limit) {
        std::vector<uint64_t> ret;
        auto s = _sorted_int_db->Range(key, cursor_id, page, limit, false, &ret);
        if(!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<uint64_t>> TitanDB::SIRevRange(const Slice &key, uint64_t cursor_id, uint64_t page, uint64_t limit) {
        std::vector<uint64_t> ret;
        auto s = _sorted_int_db->Range(key, cursor_id, page, limit, true, &ret);
        if(!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<uint64_t>> TitanDB::SIRangeByValue(const std::string_view &key, const SortedintRangeSpec& spec) {
        std::vector<uint64_t> ret;
        int uns;
        auto s = _sorted_int_db->RangeByValue(key, spec, &ret, &uns);
        if(!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if(s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

}  // namespace titandb
