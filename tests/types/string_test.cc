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

class RedisStringTest : public TestBase {
protected:
    explicit RedisStringTest() {
        string_ = std::make_unique<titandb::RedisDB>(storage_, "string_ns");
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

    std::unique_ptr<titandb::RedisDB> string_;
    std::vector<titandb::StringPair> pairs_;
};

TEST_CASE_FIXTURE(RedisStringTest, "Append") {
    int ret = 0;
    for (size_t i = 0; i < 32; i++) {
        rocksdb::Status s = string_->Append(key_, "a", &ret);
        CHECK(s.ok());
        CHECK_EQ(static_cast<int>(i + 1), ret);
    }
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "GetAndSet") {
    for (auto &pair: pairs_) {
        string_->Set(pair.key, pair.value);
    }
    for (auto &pair: pairs_) {
        std::string got_value;
        string_->Get(pair.key, &got_value);
        CHECK_EQ(pair.value, got_value);
    }
    for (auto &pair: pairs_) {
        string_->Del(pair.key);
    }
}

TEST_CASE_FIXTURE(RedisStringTest, "MGetAndMSet") {
    string_->MSet(pairs_);
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

TEST_CASE_FIXTURE(RedisStringTest, "IncrByFloat") {
    double f = 0.0;
    double max_float = std::numeric_limits<double>::max();
    string_->IncrByFloat(key_, 1.0, &f);
    CHECK_EQ(1.0, f);
    string_->IncrByFloat(key_, max_float - 1, &f);
    CHECK_EQ(max_float, f);
    string_->IncrByFloat(key_, 1.2, &f);
    CHECK_EQ(max_float, f);
    string_->IncrByFloat(key_, -1 * max_float, &f);
    CHECK_EQ(0, f);
    string_->IncrByFloat(key_, -1 * max_float, &f);
    CHECK_EQ(-1 * max_float, f);
    string_->IncrByFloat(key_, -1.2, &f);
    CHECK_EQ(-1 * max_float, f);
    // key hold value is not the number
    string_->Set(key_, "abc");
    rocksdb::Status s = string_->IncrByFloat(key_, 1.2, &f);
    CHECK(s.IsInvalidArgument());
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "IncrBy") {
    int64_t ret = 0;
    string_->IncrBy(key_, 1, &ret);
    CHECK_EQ(1, ret);
    string_->IncrBy(key_, INT64_MAX - 1, &ret);
    CHECK_EQ(INT64_MAX, ret);
    rocksdb::Status s = string_->IncrBy(key_, 1, &ret);
    CHECK(s.IsInvalidArgument());
    string_->IncrBy(key_, INT64_MIN + 1, &ret);
    CHECK_EQ(0, ret);
    string_->IncrBy(key_, INT64_MIN, &ret);
    CHECK_EQ(INT64_MIN, ret);
    s = string_->IncrBy(key_, -1, &ret);
    CHECK(s.IsInvalidArgument());
    // key hold value is not the number
    string_->Set(key_, "abc");
    s = string_->IncrBy(key_, 1, &ret);
    CHECK(s.IsInvalidArgument());
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "GetEmptyValue") {
    const std::string key = "empty_value_key";
    auto s = string_->Set(key, "");
    CHECK(s.ok());
    std::string value;
    s = string_->Get(key, &value);
    CHECK((s.ok() && value.empty()));
}

TEST_CASE_FIXTURE(RedisStringTest, "GetSet") {
    int64_t ttl = 0;
    int64_t now = 0;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    std::vector<std::string> values = {"a", "b", "c", "d"};
    for (size_t i = 0; i < values.size(); i++) {
        std::string old_value;
        string_->Expire(key_, now * 1000 + 100000);
        string_->GetSet(key_, values[i], &old_value);
        if (i != 0) {
            CHECK_EQ(values[i - 1], old_value);
            string_->TTL(key_, &ttl);
            CHECK(ttl == -1);
        } else {
            CHECK(old_value.empty());
        }
    }
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "GetDel") {
    for (auto &pair: pairs_) {
        string_->Set(pair.key, pair.value);
    }
    for (auto &pair: pairs_) {
        std::string got_value;
        string_->GetDel(pair.key, &got_value);
        CHECK_EQ(pair.value, got_value);

        std::string second_got_value;
        auto s = string_->GetDel(pair.key, &second_got_value);
        CHECK((!s.ok() && s.IsNotFound()));
    }
}

TEST_CASE_FIXTURE(RedisStringTest, "MSetXX") {
    int ret = 0;
    string_->SetXX(key_, "test-value", 3000, &ret);
    CHECK_EQ(ret, 0);
    string_->Set(key_, "test-value");
    string_->SetXX(key_, "test-value", 3000, &ret);
    CHECK_EQ(ret, 1);
    int64_t ttl = 0;
    string_->TTL(key_, &ttl);
    CHECK((ttl >= 2000 && ttl <= 4000));
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "MSetNX") {
    int ret = 0;
    string_->MSetNX(pairs_, 0, &ret);
    CHECK_EQ(1, ret);
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

    std::vector<titandb::StringPair> new_pairs{
            {"a",           "1"},
            {"b",           "2"},
            {"c",           "3"},
            {pairs_[0].key, pairs_[0].value},
            {"d",           "4"},
    };
    string_->MSetNX(pairs_, 0, &ret);
    CHECK_EQ(0, ret);

    for (auto &pair: pairs_) {
        string_->Del(pair.key);
    }
}

TEST_CASE_FIXTURE(RedisStringTest, "MSetNXWithTTL") {
    int ret = 0;
    string_->SetNX(key_, "test-value", 3000, &ret);
    int64_t ttl = 0;
    string_->TTL(key_, &ttl);
    CHECK((ttl >= 2000 && ttl <= 4000));
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "SetEX") {
    string_->SetEX(key_, "test-value", 3000);
    int64_t ttl = 0;
    string_->TTL(key_, &ttl);
    CHECK((ttl >= 2000 && ttl <= 4000));
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "SetRange") {
    int ret = 0;
    string_->Set(key_, "hello,world");
    string_->SetRange(key_, 6, "redis", &ret);
    CHECK_EQ(11, ret);
    std::string value;
    string_->Get(key_, &value);
    CHECK_EQ("hello,redis", value);

    string_->SetRange(key_, 6, "test", &ret);
    CHECK_EQ(11, ret);
    string_->Get(key_, &value);
    CHECK_EQ("hello,tests", value);

    string_->SetRange(key_, 6, "redis-1234", &ret);
    string_->Get(key_, &value);
    CHECK_EQ("hello,redis-1234", value);

    string_->SetRange(key_, 15, "1", &ret);
    CHECK_EQ(16, ret);
    string_->Get(key_, &value);
    CHECK_EQ(16, value.size());
    string_->Del(key_);
}

TEST_CASE_FIXTURE(RedisStringTest, "CAS") {
    int ret = 0;
    std::string key = "cas_key", value = "cas_value", new_value = "new_value";

    auto status = string_->Set(key, value);
    REQUIRE(status.ok());

    status = string_->CAS("non_exist_key", value, new_value, 10000, &ret);
    REQUIRE(status.ok());
    CHECK_EQ(-1, ret);

    status = string_->CAS(key, "cas_value_err", new_value, 10000, &ret);
    REQUIRE(status.ok());
    CHECK_EQ(0, ret);

    status = string_->CAS(key, value, new_value, 10000, &ret);
    REQUIRE(status.ok());
    CHECK_EQ(1, ret);

    std::string current_value;
    status = string_->Get(key, &current_value);
    REQUIRE(status.ok());
    CHECK_EQ(new_value, current_value);

    int64_t ttl = 0;
    string_->TTL(key, &ttl);
    CHECK((ttl >= 9000 && ttl <= 11000));

    string_->Del(key);
}

TEST_CASE_FIXTURE(RedisStringTest, "CAD") {
    int ret = 0;
    std::string key = "cas_key", value = "cas_value";

    auto status = string_->Set(key, value);
    REQUIRE(status.ok());

    status = string_->CAD("non_exist_key", value, &ret);
    REQUIRE(status.ok());
    CHECK_EQ(-1, ret);

    status = string_->CAD(key, "cas_value_err", &ret);
    REQUIRE(status.ok());
    CHECK_EQ(0, ret);

    status = string_->CAD(key, value, &ret);
    REQUIRE(status.ok());
    CHECK_EQ(1, ret);

    std::string current_value;
    status = string_->Get(key, &current_value);
    REQUIRE(status.IsNotFound());

    string_->Del(key);
}