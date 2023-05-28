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

    turbo::ResultStatus<int> TitanDB::LPush(const std::string_view &key, const std::vector<std::string_view> &items) {
        int ret;
        rocksdb::Status s = _list_db->Push(key, items, true, &ret);

        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::RPush(const std::string_view &key, const std::vector<std::string_view> &items) {
        int ret;
        rocksdb::Status s = _list_db->Push(key, items, false, &ret);

        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::LPushX(const std::string_view &key, const std::vector<std::string_view> &items) {
        int ret;
        rocksdb::Status s = _list_db->PushX(key, items, true, &ret);

        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::RPushX(const std::string_view &key, const std::vector<std::string_view> &items) {
        int ret;
        rocksdb::Status s = _list_db->PushX(key, items, false, &ret);

        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::string> TitanDB::LPop(const std::string_view &key) {
        std::string ret;
        rocksdb::Status s = _list_db->Pop(key, true, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::LPop(const std::string_view &key, int32_t count) {
        std::vector<std::string> ret;
        rocksdb::Status s = _list_db->PopMulti(key, true, count, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::string> TitanDB::RPop(const std::string_view &key) {
        std::string ret;
        rocksdb::Status s = _list_db->Pop(key, false, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::RPop(const std::string_view &key, int32_t count) {
        std::vector<std::string> ret;
        rocksdb::Status s = _list_db->PopMulti(key, false, count, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::LRem(const std::string_view &key, const std::string_view &elem, uint32_t count) {
        int ret;
        rocksdb::Status s = _list_db->Rem(key, count, elem, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int>
    TitanDB::LInsert(const std::string_view &key, const std::string_view &pov, const std::string_view &elm,
                     bool before) {
        int ret;
        rocksdb::Status s = _list_db->Insert(key, pov, elm, before, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<std::string>> TitanDB::LRange(const std::string_view &key, int start, int stop) {
        std::vector<std::string> ret;
        rocksdb::Status s = _list_db->Range(key, start, stop, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::string> TitanDB::LIndex(const std::string_view &key, int index) {
        std::string ret;
        rocksdb::Status s = _list_db->Index(key, index, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::Status TitanDB::LTrim(const std::string_view &key, int start, int stop) {
        rocksdb::Status s = _list_db->Trim(key, start, stop);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return turbo::OkStatus();
    }

    turbo::Status TitanDB::LSet(const std::string_view &key, const std::string_view &value, int index) {
        rocksdb::Status s = _list_db->Set(key, index, value);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return turbo::OkStatus();
    }

    turbo::ResultStatus<std::string> TitanDB::RPopPush(const std::string_view &src, const std::string_view &dst) {
        std::string ret;
        rocksdb::Status s = _list_db->RPopLPush(src, dst, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::string>
    TitanDB::LMove(const std::string_view &src, const std::string_view &dst, bool src_left, bool dst_left) {
        std::string ret;
        rocksdb::Status s = _list_db->LMove(src, dst, src_left, dst_left, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }
}  // namespace titandb
