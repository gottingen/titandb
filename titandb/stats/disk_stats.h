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

#include "titandb/storage/redis_db.h"
#include "titandb/storage/redis_metadata.h"

namespace titandb {

    class Disk : public RedisDB {
    public:
        explicit Disk(Storage *storage, const std::string &ns) : RedisDB(storage, ns) {
            option_.include_memtabtles = true;
            option_.include_files = true;
        }

        rocksdb::Status GetApproximateSizes(const Metadata &metadata, const Slice &ns_key,
                                            rocksdb::ColumnFamilyHandle *column_family, uint64_t *key_size,
                                            Slice subkeyleft = Slice(), Slice subkeyright = Slice());

        rocksdb::Status GetStringSize(const Slice &ns_key, uint64_t *key_size);

        rocksdb::Status GetHashSize(const Slice &ns_key, uint64_t *key_size);

        rocksdb::Status GetSetSize(const Slice &ns_key, uint64_t *key_size);

        rocksdb::Status GetListSize(const Slice &ns_key, uint64_t *key_size);

        rocksdb::Status GetZsetSize(const Slice &ns_key, uint64_t *key_size);

        rocksdb::Status GetBitmapSize(const Slice &ns_key, uint64_t *key_size);

        rocksdb::Status GetSortedintSize(const Slice &ns_key, uint64_t *key_size);

        rocksdb::Status GetKeySize(const Slice &user_key, RedisType type, uint64_t *key_size);

    private:
        rocksdb::SizeApproximationOptions option_;
    };

}  // namespace titandb
