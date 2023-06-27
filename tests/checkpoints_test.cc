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

#include "test_base.h"
#include "titandb/types/redis_string.h"

class RedisStringTest : public TestBase {
protected:
    explicit RedisStringTest() {
        string_ = std::make_unique<titandb::RedisString>(storage_, "string_ns");
        key_ = "test-string-key";
        pairs_ = {
                {"test-string-key1", "test-strings-value1"},
                {"test-string-key2", "test-strings-value2"},
                {"test-string-key3", "test-strings-value3"},
                {"test-string-key4", "test-strings-value4"},
                {"test-string-key5", "test-strings-value5"},
                {"test-string-key6", "test-strings-value6"},
        };
    }

    ~RedisStringTest() override = default;

    std::unique_ptr<titandb::RedisString> string_;
    std::vector<titandb::StringPair> pairs_;
};



TEST_CASE_FIXTURE(RedisStringTest, "MGetAndMSet") {
    string_->MSet(pairs_);
    auto s = storage_->CreateCheckPoint("check_point_dir");
    REQUIRE(s.ok());
    string_->Del("test-string-key6");
    std::string va;
    auto ss = string_->Get("test-string-key6", &va);
    REQUIRE_FALSE((ss.ok() && ss.IsNotFound()));
    REQUIRE(storage_->ExistCheckpoint("check_point_dir"));
    auto sr = storage_->RestoreFromCheckpoint("check_point_dir");
    string_->Refresh();
    std::cout<<sr.ToString()<<std::endl;
    REQUIRE(sr.ok());
    std::vector<std::string_view> keys;
    std::vector<std::string> values;
    keys.reserve(pairs_.size());
    for (const auto &pair: pairs_) {
        keys.emplace_back(pair.key);
    }
    string_->MGet(keys, &values);
    for (size_t i = 0; i < pairs_.size(); i++) {
        CHECK_EQ(pairs_[i].value, values[i]);
    }
    for (auto &pair: pairs_) {
        string_->Del(pair.key);
    }
}
