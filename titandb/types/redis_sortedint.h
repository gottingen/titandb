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

#include <limits>
#include <string>
#include <vector>

#include "titandb/storage/redis_db.h"
#include "titandb/storage/redis_metadata.h"


namespace titandb {
    struct SortedintRangeSpec {
        uint64_t min = std::numeric_limits<uint64_t>::lowest(), max = std::numeric_limits<uint64_t>::max();
        bool minex = false, maxex = false; /* are min or max exclusive */
        int offset = -1, count = -1;
        bool reversed = false;

        SortedintRangeSpec() = default;
    };


    class RedisSortedInt : public RedisDB {
    public:
        explicit RedisSortedInt(Storage *storage, const std::string &ns) : RedisDB(storage, ns) {}

        rocksdb::Status Card(const Slice &user_key, int *ret);

        rocksdb::Status MExist(const Slice &user_key, const std::vector<uint64_t> &ids, std::vector<int> *exists);

        rocksdb::Status Add(const Slice &user_key, const std::vector<uint64_t> &ids, int *ret);

        rocksdb::Status Remove(const Slice &user_key, const std::vector<uint64_t> &ids, int *ret);

        rocksdb::Status Range(const Slice &user_key, uint64_t cursor_id, uint64_t page, uint64_t limit, bool reversed,
                              std::vector<uint64_t> *ids);

        rocksdb::Status
        RangeByValue(const Slice &user_key, SortedintRangeSpec spec, std::vector<uint64_t> *ids, int *size);

        static turbo::Status ParseRangeSpec(const std::string &min, const std::string &max, SortedintRangeSpec *spec);

    private:
        rocksdb::Status GetMetadata(const Slice &ns_key, SortedintMetadata *metadata);
    };

}  // namespace titandb
