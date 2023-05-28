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

#if defined(__sparc__) || defined(__arm__)
#define USE_ALIGNED_ACCESS
#endif

namespace titandb {
    enum BitOpFlags {
        kBitOpAnd,
        kBitOpOr,
        kBitOpXor,
        kBitOpNot,
    };

    class RedisBitmap : public RedisDB {
    public:
        RedisBitmap(Storage *storage, const std::string &ns) : RedisDB(storage, ns) {}

        rocksdb::Status GetBit(const Slice &user_key, uint32_t offset, bool *bit);

        rocksdb::Status GetString(const Slice &user_key, uint32_t max_btos_size, std::string *value);

        rocksdb::Status SetBit(const Slice &user_key, uint32_t offset, bool new_bit, bool *old_bit);

        rocksdb::Status BitCount(const Slice &user_key, int64_t start, int64_t stop, uint32_t *cnt);

        rocksdb::Status
        BitPos(const Slice &user_key, bool bit, int64_t start, int64_t stop, bool stop_given, int64_t *pos);

        rocksdb::Status BitOp(BitOpFlags op_flag, const std::string_view &op_name, const std::string_view &user_key,
                              const std::vector<std::string_view> &op_keys, int64_t *len);

        static bool GetBitFromValueAndOffset(const std::string &value, uint32_t offset);

        static bool IsEmptySegment(const Slice &segment);

    private:
        rocksdb::Status GetMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value);
    };

}  // namespace titandb
