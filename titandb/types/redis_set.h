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

#include <string>
#include <vector>

#include "titandb/storage/redis_db.h"
#include "titandb/storage/redis_metadata.h"

namespace titandb {

    class RedisSet : public SubKeyScanner {
    public:
        explicit RedisSet(Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}

        rocksdb::Status Card(const std::string_view &user_key, int *ret);

        rocksdb::Status IsMember(const std::string_view &user_key, const std::string_view &member, int *ret);

        rocksdb::Status MIsMember(const std::string_view &user_key, const std::vector<std::string_view> &members, std::vector<int> *exists);

        rocksdb::Status Add(const std::string_view &user_key, const std::vector<std::string_view> &members, int *ret);

        rocksdb::Status Remove(const std::string_view &user_key, const std::vector<std::string_view> &members, int *ret);

        rocksdb::Status Members(const std::string_view &user_key, std::vector<std::string> *members);

        rocksdb::Status Move(const std::string_view &src, const std::string_view &dst, const std::string_view &member, int *ret);

        rocksdb::Status Take(const std::string_view &user_key, std::vector<std::string> *members, int count, bool pop);

        rocksdb::Status Diff(const std::vector<std::string_view> &keys, std::vector<std::string> *members);

        rocksdb::Status Union(const std::vector<std::string_view> &keys, std::vector<std::string> *members);

        rocksdb::Status Inter(const std::vector<std::string_view> &keys, std::vector<std::string> *members);

        rocksdb::Status Overwrite(std::string_view user_key, const std::vector<std::string> &members);

        rocksdb::Status DiffStore(const std::string_view &dst, const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status UnionStore(const std::string_view &dst, const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status InterStore(const std::string_view &dst, const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status Scan(const std::string_view &user_key, const std::string_view &cursor, uint64_t limit,
                             const std::string_view &member_prefix, std::vector<std::string> *members);

    private:
        rocksdb::Status GetMetadata(const std::string_view &ns_key, SetMetadata *metadata);
    };

}  // namespace titandb
