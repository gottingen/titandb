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

#include <memory>

#include "../test_base.h"
#include "titandb/redis_db.h"

class RedisBitmapTest : public TestBase {
protected:
    explicit RedisBitmapTest() { bitmap_ = std::make_unique<titandb::RedisDB>(storage_, "bitmap_ns"); key_ = "test_bitmap_key"; }

    ~RedisBitmapTest() override = default;

    std::unique_ptr<titandb::RedisDB> bitmap_;
};

TEST_CASE_FIXTURE(RedisBitmapTest, "GetAndSetBit") {
    uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
    for (const auto &offset: offsets) {
        bool bit = false;
        bitmap_->GetBit(key_, offset, &bit);
        CHECK_FALSE(bit);
        bitmap_->SetBit(key_, offset, true, &bit);
        bitmap_->GetBit(key_, offset, &bit);
        CHECK(bit);
    }
    bitmap_->Del(key_);
}

TEST_CASE_FIXTURE(RedisBitmapTest, "BitCount") {
    uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
    for (const auto &offset: offsets) {
        bool bit = false;
        bitmap_->SetBit(key_, offset, true, &bit);
    }
    uint32_t cnt = 0;
    bitmap_->BitCount(key_, 0, 4 * 1024, &cnt);
    CHECK_EQ(cnt, 6);
    bitmap_->BitCount(key_, 0, -1, &cnt);
    CHECK_EQ(cnt, 6);
    bitmap_->Del(key_);
}

TEST_CASE_FIXTURE(RedisBitmapTest, "BitPosClearBit") {
    int64_t pos = 0;
    bool old_bit = false;
    for (int i = 0; i < 1024 + 16; i++) {
        bitmap_->BitPos(key_, false, 0, -1, true, &pos);
        CHECK_EQ(pos, i);
        bitmap_->SetBit(key_, i, true, &old_bit);
        CHECK_FALSE(old_bit);
    }
    bitmap_->Del(key_);
}

TEST_CASE_FIXTURE(RedisBitmapTest, "BitPosSetBit") {
    uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 16, 3 * 1024 * 8, 3 * 1024 * 8 + 16};
    for (const auto &offset: offsets) {
        bool bit = false;
        bitmap_->SetBit(key_, offset, true, &bit);
    }
    int64_t pos = 0;
    int start_indexes[] = {0, 1, 124, 1025, 1027, 3 * 1024 + 1};
    for (size_t i = 0; i < sizeof(start_indexes) / sizeof(start_indexes[0]); i++) {
        bitmap_->BitPos(key_, true, start_indexes[i], -1, true, &pos);
        CHECK_EQ(pos, offsets[i]);
    }
    bitmap_->Del(key_);
}
