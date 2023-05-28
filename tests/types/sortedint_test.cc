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

#include <gtest/gtest.h>

#include <memory>

#include "../test_base.h"
#include "titandb/types/redis_sortedint.h"

using namespace titandb;

class RedisSortedintTest : public TestBase {
protected:
    explicit RedisSortedintTest() { sortedint_ = std::make_unique<titandb::RedisSortedInt>(storage_, "sortedint_ns"); }

    ~RedisSortedintTest() override = default;

    void SetUp() override {
        key_ = "test-sortedint-key";
        ids_ = {1, 2, 3, 4};
    }

    std::unique_ptr<titandb::RedisSortedInt> sortedint_;
    std::vector<uint64_t> ids_;
};

TEST_F(RedisSortedintTest, AddAndRemove) {
    int ret = 0;
    rocksdb::Status s = sortedint_->Add(key_, ids_, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
    s = sortedint_->Card(key_, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
    s = sortedint_->Remove(key_, ids_, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
    s = sortedint_->Card(key_, &ret);
    EXPECT_TRUE(s.ok() && ret == 0);
    sortedint_->Del(key_);
}

TEST_F(RedisSortedintTest, Range) {
    int ret = 0;
    rocksdb::Status s = sortedint_->Add(key_, ids_, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
    std::vector<uint64_t> ids;
    s = sortedint_->Range(key_, 0, 0, 20, false, &ids);
    EXPECT_TRUE(s.ok() && ids_.size() == ids.size());
    for (size_t i = 0; i < ids_.size(); i++) {
        EXPECT_EQ(ids_[i], ids[i]);
    }
    s = sortedint_->Remove(key_, ids_, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(ids_.size()) == ret);
    sortedint_->Del(key_);
}
