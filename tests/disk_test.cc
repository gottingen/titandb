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

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest/doctest.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "titandb/stats/disk_stats.h"
#include "titandb/storage/redis_metadata.h"
#include "test_base.h"
#include "titandb/types/redis_bitmap.h"
#include "titandb/types/redis_list.h"
#include "titandb/types/redis_set.h"
#include "titandb/types/redis_sortedint.h"
#include "titandb/types/redis_string.h"
#include "titandb/types/redis_zset.h"

using namespace titandb;
/*
class RedisDiskTest : public TestBase {
protected:
    explicit RedisDiskTest() = default;

    ~RedisDiskTest() override = default;

    double estimation_factor_ = 0.1;
};


TEST_F(RedisDiskTest, StringDisk) {
    key_ = "stringdisk_key";
    std::unique_ptr<titandb::RedisString> string = std::make_unique<titandb::RedisString>(storage_, "disk_ns_string");
    std::unique_ptr<titandb::Disk> disk = std::make_unique<titandb::Disk>(storage_, "disk_ns_string");
    std::vector<int> value_size{1024 * 1024};
    CHECK(string->Set(key_, std::string(value_size[0], 'a')).ok());
    uint64_t result = 0;
    CHECK(disk->GetKeySize(key_, kRedisString, &result).ok());
    EXPECT_GE(result, value_size[0] * estimation_factor_);
    EXPECT_LE(result, value_size[0] / estimation_factor_);
    string->Del(key_);
}

TEST_F(RedisDiskTest, HashDisk) {
    std::unique_ptr<titandb::RedisHash> hash = std::make_unique<titandb::RedisHash>(storage_, "disk_ns_hash");
    std::unique_ptr<titandb::Disk> disk = std::make_unique<titandb::Disk>(storage_, "disk_ns_hash");
    key_ = "hashdisk_key";
    fields_ = {"hashdisk_kkey1", "hashdisk_kkey2", "hashdisk_kkey3", "hashdisk_kkey4", "hashdisk_kkey5"};
    values_.resize(5);
    uint64_t approximate_size = 0;
    int ret = 0;
    std::vector<int> value_size{1024, 1024, 1024, 1024, 1024};
    std::vector<std::string> values(value_size.size());
    for (int i = 0; i < int(fields_.size()); i++) {
        values[i] = std::string(value_size[i], static_cast<char>('a' + i));
        values_[i] = values[i];
        approximate_size += key_.size() + 8 + fields_[i].size() + values_[i].size();
        rocksdb::Status s = hash->Set(key_, fields_[i], values_[i], &ret);
        CHECK(s.ok() && ret == 1);
    }
    uint64_t key_size = 0;
    CHECK(disk->GetKeySize(key_, kRedisHash, &key_size).ok());
    EXPECT_GE(key_size, approximate_size * estimation_factor_);
    EXPECT_LE(key_size, approximate_size / estimation_factor_);
    hash->Del(key_);
}

TEST_F(RedisDiskTest, SetDisk) {
    std::unique_ptr<titandb::RedisSet> set = std::make_unique<titandb::RedisSet>(storage_, "disk_ns_set");
    std::unique_ptr<titandb::Disk> disk = std::make_unique<titandb::Disk>(storage_, "disk_ns_set");
    key_ = "setdisk_key";
    values_.resize(5);
    uint64_t approximate_size = 0;
    int ret = 0;
    std::vector<int> value_size{1024, 1024, 1024, 1024, 1024};
    std::vector<std::string> values(value_size.size());
    for (int i = 0; i < int(values_.size()); i++) {
        values[i] = std::string(value_size[i], static_cast<char>(i + 'a'));
        values_[i] = values[i];
        approximate_size += key_.size() + values_[i].size() + 8;
    }
    rocksdb::Status s = set->Add(key_, values_, &ret);
    CHECK(s.ok() && ret == 5);

    uint64_t key_size = 0;
    CHECK(disk->GetKeySize(key_, kRedisSet, &key_size).ok());
    EXPECT_GE(key_size, approximate_size * estimation_factor_);
    EXPECT_LE(key_size, approximate_size / estimation_factor_);

    set->Del(key_);
}

TEST_F(RedisDiskTest, ListDisk) {
    std::unique_ptr<titandb::RedisList> list = std::make_unique<titandb::RedisList>(storage_, "disk_ns_list");
    std::unique_ptr<titandb::Disk> disk = std::make_unique<titandb::Disk>(storage_, "disk_ns_list");
    key_ = "listdisk_key";
    values_.resize(5);
    std::vector<int> value_size{1024, 1024, 1024, 1024, 1024};
    std::vector<std::string> values(value_size.size());
    uint64_t approximate_size = 0;
    for (int i = 0; i < int(values_.size()); i++) {
        values[i] = std::string(value_size[i], static_cast<char>('a' + i));
        values_[i] = values[i];
        approximate_size += key_.size() + values_[i].size() + 8 + 8;
    }
    int ret = 0;
    rocksdb::Status s = list->Push(key_, values_, false, &ret);
    CHECK(s.ok() && ret == 5);
    uint64_t key_size = 0;
    CHECK(disk->GetKeySize(key_, kRedisList, &key_size).ok());
    EXPECT_GE(key_size, approximate_size * estimation_factor_);
    EXPECT_LE(key_size, approximate_size / estimation_factor_);
    list->Del(key_);
}


TEST_F(RedisDiskTest, BitmapDisk) {
    std::unique_ptr<titandb::RedisBitmap> bitmap = std::make_unique<titandb::RedisBitmap>(storage_, "disk_ns_bitmap");
    std::unique_ptr<titandb::Disk> disk = std::make_unique<titandb::Disk>(storage_, "disk_ns_bitmap");
    key_ = "bitmapdisk_key";
    bool bit = false;
    uint64_t approximate_size = 0;
    for (int i = 0; i < 1024 * 8 * 100000; i += 1024 * 8) {
        CHECK(bitmap->SetBit(key_, i, true, &bit).ok());
        approximate_size += key_.size() + 8 + std::to_string(i / 1024 / 8).size();
    }
    uint64_t key_size = 0;
    CHECK(disk->GetKeySize(key_, kRedisBitmap, &key_size).ok());
    EXPECT_GE(key_size, approximate_size * estimation_factor_);
    EXPECT_LE(key_size, approximate_size / estimation_factor_);
    bitmap->Del(key_);
}

TEST_F(RedisDiskTest, SortedintDisk) {
    std::unique_ptr<titandb::RedisSortedInt> sortedint = std::make_unique<titandb::RedisSortedInt>(storage_, "disk_ns_sortedint");
    std::unique_ptr<titandb::Disk> disk = std::make_unique<titandb::Disk>(storage_, "disk_ns_sortedint");
    key_ = "sortedintdisk_key";
    int ret = 0;
    uint64_t approximate_size = 0;
    for (int i = 0; i < 100000; i++) {
        CHECK(sortedint->Add(key_, std::vector<uint64_t>{uint64_t(i)}, &ret).ok() && ret == 1);
        approximate_size += key_.size() + 8 + 8;
    }
    uint64_t key_size = 0;
    CHECK(disk->GetKeySize(key_, kRedisSortedint, &key_size).ok());
    EXPECT_GE(key_size, approximate_size * estimation_factor_);
    EXPECT_LE(key_size, approximate_size / estimation_factor_);
    sortedint->Del(key_);
}

TEST_F(RedisDiskTest, StreamDisk) {
    std::unique_ptr<titandb::RedisStream> stream = std::make_unique<titandb::RedisStream>(storage_, "disk_ns_stream");
    std::unique_ptr<titandb::Disk> disk = std::make_unique<titandb::Disk>(storage_, "disk_ns_stream");
    key_ = "streamdisk_key";
    titandb::StreamAddOptions options;
    options.next_id_strategy = *titandb::ParseNextStreamEntryIDStrategy("*");
    titandb::StreamEntryID id;
    uint64_t approximate_size = 0;
    for (int i = 0; i < 100000; i++) {
        std::vector<std::string> values = {"key" + std::to_string(i), "val" + std::to_string(i)};
        auto s = stream->Add(key_, options, values, &id);
        CHECK(s.ok());
        approximate_size += key_.size() + 8 + 8 + values[0].size() + values[1].size();
    }
    uint64_t key_size = 0;
    CHECK(disk->GetKeySize(key_, kRedisStream, &key_size).ok());
    EXPECT_GE(key_size, approximate_size * estimation_factor_);
    EXPECT_LE(key_size, approximate_size / estimation_factor_);
    stream->Del(key_);
}*/