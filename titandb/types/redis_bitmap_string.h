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

    class RedisBitmapString : public RedisDB {
    public:
        RedisBitmapString(Storage *storage, const std::string &ns) : RedisDB(storage, ns) {}

        static rocksdb::Status GetBit(const std::string &raw_value, uint32_t offset, bool *bit);

        rocksdb::Status
        SetBit(const Slice &ns_key, std::string *raw_value, uint32_t offset, bool new_bit, bool *old_bit);

        static rocksdb::Status BitCount(const std::string &raw_value, int64_t start, int64_t stop, uint32_t *cnt);

        static rocksdb::Status
        BitPos(const std::string &raw_value, bool bit, int64_t start, int64_t stop, bool stop_given,
               int64_t *pos);

        static size_t RawPopcount(const uint8_t *p, int64_t count);

        static int64_t RawBitpos(const uint8_t *c, int64_t count, bool bit);
    };

}  // namespace titandb
