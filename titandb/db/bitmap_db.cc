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

    turbo::ResultStatus<bool> TitanDB::GetBit(const std::string_view &key, uint32_t offset) {
        bool value;
        auto s = _db->GetBit(key, offset, &value);
        if (!s.ok()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        return value;
    }

    turbo::ResultStatus<bool> TitanDB::SetBit(const std::string_view &key, uint32_t offset, bool flag) {
        bool value;
        auto s = _db->SetBit(key, offset, flag, &value);
        if (!s.ok()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        return value;

    }

    turbo::ResultStatus<uint32_t> TitanDB::BitCount(const std::string_view &key, int64_t start, int64_t stop) {
        uint32_t value;
        auto s = _db->BitCount(key, start, stop, &value);
        if (!s.ok()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        return value;
    }

    turbo::ResultStatus<int64_t> TitanDB::BitPos(const std::string_view &key, bool flag, int64_t start, int64_t stop) {
        int64_t value;
        bool stop_given = (stop != -1);
        auto s = _db->BitPos(key, flag, start, stop, stop_given, &value);
        if (!s.ok()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        return value;
    }

    turbo::ResultStatus<int64_t>
    TitanDB::BitOP(BitOpFlags op, const std::string_view &dest_key, const std::string_view &key,
                   const std::vector<std::string_view> &keys) {
        int64_t value;
        auto s = _db->BitOp(op, dest_key, key, keys, &value);
        if (!s.ok()) {
            return turbo::Status(turbo::kUnavailable, s.ToString());
        }
        return value;
    }
}  // namespace titandb
