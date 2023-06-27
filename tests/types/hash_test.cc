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

#include <algorithm>
#include <climits>
#include <memory>
#include <random>
#include <string>

#include "turbo/strings/numbers.h"
#include "../test_base.h"
#include "titandb/redis_db.h"

class RedisHashTest : public TestBase {
protected:
    explicit RedisHashTest() {
        hash_ = std::make_unique<titandb::RedisDB>(storage_, "hash_ns");
        key_ = "test_hash->key";
        fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
        values_ = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
    }

    ~RedisHashTest() override = default;
    

    std::unique_ptr<titandb::RedisDB> hash_;
};

TEST_CASE_FIXTURE(RedisHashTest, "GetAndSet") {
    int ret = 0;
    for (size_t i = 0; i < fields_.size(); i++) {
        auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
        CHECK((s.ok() && ret == 1));
    }
    for (size_t i = 0; i < fields_.size(); i++) {
        std::string got;
        auto s = hash_->Get(key_, fields_[i], &got);
        CHECK_EQ(s.ToString(), "OK");
        CHECK_EQ(values_[i], got);
    }
    auto s = hash_->Delete(key_, fields_, &ret);
    CHECK((s.ok() && static_cast<int>(fields_.size()) == ret));
    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "MGetAndMSet") {
    int ret = 0;
    std::vector<titandb::FieldValue> fvs;
    for (size_t i = 0; i < fields_.size(); i++) {
        fvs.emplace_back(std::string{fields_[i].data(), fields_[i].size()}, std::string{values_[i].data(), values_[i].size()});
    }
    auto s = hash_->MSet(key_, fvs, false, &ret);
    CHECK((s.ok() && static_cast<int>(fvs.size()) == ret));
    s = hash_->MSet(key_, fvs, false, &ret);
    CHECK(s.ok());
    CHECK_EQ(ret, 0);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    s = hash_->MGet(key_, fields_, &values, &statuses);
    CHECK(s.ok());
    for (size_t i = 0; i < fields_.size(); i++) {
        CHECK_EQ(values[i], values_[i]);
    }
    s = hash_->Delete(key_, fields_, &ret);
    CHECK(s.ok());
    CHECK_EQ(static_cast<int>(fields_.size()), ret);
    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "MSetSingleFieldAndNX") {
    int ret = 0;
    std::vector<titandb::FieldValue> values = {{"field-one", "value-one"}};
    auto s = hash_->MSet(key_, values, true, &ret);
    CHECK((s.ok() && ret == 1));

    std::string field2 = "field-two";
    std::string initial_value = "value-two";
    s = hash_->Set(key_, field2, initial_value, &ret);
    CHECK((s.ok() && ret == 1));

    values = {{field2, "value-two-changed"}};
    s = hash_->MSet(key_, values, true, &ret);
    CHECK((s.ok() && ret == 0));

    std::string final_value;
    s = hash_->Get(key_, field2, &final_value);
    CHECK(s.ok());
    CHECK_EQ(initial_value, final_value);

    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "MSetMultipleFieldsAndNX") {
    int ret = 0;
    std::vector<titandb::FieldValue> values = {{"field-one", "value-one"},
                                               {"field-two", "value-two"}};
    auto s = hash_->MSet(key_, values, true, &ret);
    CHECK((s.ok() && ret == 2));

    values = {{"field-one",   "value-one"},
              {"field-two",   "value-two-changed"},
              {"field-three", "value-three"}};
    s = hash_->MSet(key_, values, true, &ret);
    CHECK(s.ok());
    CHECK_EQ(ret, 1);

    std::string value;
    s = hash_->Get(key_, "field-one", &value);
    CHECK((s.ok() && value == "value-one"));

    s = hash_->Get(key_, "field-two", &value);
    CHECK((s.ok() && value == "value-two"));

    s = hash_->Get(key_, "field-three", &value);
    CHECK((s.ok() && value == "value-three"));

    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "HGetAll") {
    int ret = 0;
    for (size_t i = 0; i < fields_.size(); i++) {
        auto s = hash_->Set(key_, fields_[i], values_[i], &ret);
        CHECK((s.ok() && ret == 1));
    }
    std::vector<titandb::FieldValue> fvs;
    auto s = hash_->GetAll(key_, &fvs);
    CHECK((s.ok() && fvs.size() == fields_.size()));
    s = hash_->Delete(key_, fields_, &ret);
    CHECK((s.ok() && static_cast<int>(fields_.size()) == ret));
    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "HIncr") {
    int64_t value = 0;
    rocksdb::Slice field("hash-incrby-invalid-field");
    for (int i = 0; i < 32; i++) {
        auto s = hash_->IncrBy(key_, field, 1, &value);
        CHECK(s.ok());
    }
    std::string bytes;
    hash_->Get(key_, field, &bytes);
    int64_t n;
    if (!turbo::SimpleAtoi(bytes, &n)) {
        FAIL("");
    }
    CHECK_EQ(32, n);
    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "HIncrInvalid") {
    int ret = 0;
    int64_t value = 0;
    rocksdb::Slice field("hash-incrby-invalid-field");
    auto s = hash_->IncrBy(key_, field, 1, &value);
    CHECK((s.ok() && value == 1));

    s = hash_->IncrBy(key_, field, LLONG_MAX, &value);
    CHECK(s.IsInvalidArgument());
    hash_->Set(key_, field, "abc", &ret);
    s = hash_->IncrBy(key_, field, 1, &value);
    CHECK(s.IsInvalidArgument());

    hash_->Set(key_, field, "-1", &ret);
    s = hash_->IncrBy(key_, field, -1, &value);
    CHECK(s.ok());
    s = hash_->IncrBy(key_, field, LLONG_MIN, &value);
    CHECK(s.IsInvalidArgument());

    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "HIncrByFloat") {
    double value = 0.0;
    rocksdb::Slice field("hash-incrbyfloat-invalid-field");
    for (int i = 0; i < 32; i++) {
        auto s = hash_->IncrByFloat(key_, field, 1.2, &value);
        CHECK(s.ok());
    }
    std::string bytes;
    hash_->Get(key_, field, &bytes);
    value = std::stof(bytes);
    CHECK_LE((32 * 1.2 -value) , 0.001);
    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "HRangeByLex") {
    int ret = 0;
    std::vector<titandb::FieldValue> fvs;
    for (size_t i = 0; i < 4; i++) {
        fvs.emplace_back("key" + std::to_string(i), "value" + std::to_string(i));
    }
    for (size_t i = 0; i < 26; i++) {
        fvs.emplace_back(std::to_string(char(i + 'a')), std::to_string(char(i + 'a')));
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::vector<titandb::FieldValue> tmp(fvs);
    for (size_t i = 0; i < 100; i++) {
        std::shuffle(tmp.begin(), tmp.end(), g);
        auto s = hash_->MSet(key_, tmp, false, &ret);
        CHECK((s.ok() && static_cast<int>(tmp.size()) == ret));
        s = hash_->MSet(key_, fvs, false, &ret);
        CHECK(s.ok());
        CHECK_EQ(ret, 0);
        std::vector<titandb::FieldValue> result;
        titandb::RangeLexSpec spec;
        spec.offset = 0;
        spec.count = INT_MAX;
        spec.min = "key0";
        spec.max = "key3";
        s = hash_->RangeByLex(key_, spec, &result);
        CHECK(s.ok());
        CHECK_EQ(4, result.size());
        CHECK_EQ("key0", result[0].field);
        CHECK_EQ("key1", result[1].field);
        CHECK_EQ("key2", result[2].field);
        CHECK_EQ("key3", result[3].field);
        hash_->Del(key_);
    }

    auto s = hash_->MSet(key_, tmp, false, &ret);
    CHECK((s.ok() && static_cast<int>(tmp.size()) == ret));
    // use offset and count
    std::vector<titandb::FieldValue> result;
    titandb::RangeLexSpec spec;
    spec.offset = 0;
    spec.count = INT_MAX;
    spec.min = "key0";
    spec.max = "key3";
    spec.offset = 1;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(3, result.size());
    CHECK_EQ("key1", result[0].field);
    CHECK_EQ("key2", result[1].field);
    CHECK_EQ("key3", result[2].field);

    spec.offset = 1;
    spec.count = 1;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(1, result.size());
    CHECK_EQ("key1", result[0].field);

    spec.offset = 0;
    spec.count = 0;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(0, result.size());

    spec.offset = 1000;
    spec.count = 1000;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(0, result.size());
    // exclusive range
    spec.offset = 0;
    spec.count = -1;
    spec.minex = true;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(3, result.size());
    CHECK_EQ("key1", result[0].field);
    CHECK_EQ("key2", result[1].field);
    CHECK_EQ("key3", result[2].field);

    spec.offset = 0;
    spec.count = -1;
    spec.maxex = true;
    spec.minex = false;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(3, result.size());
    CHECK_EQ("key0", result[0].field);
    CHECK_EQ("key1", result[1].field);
    CHECK_EQ("key2", result[2].field);

    spec.offset = 0;
    spec.count = -1;
    spec.maxex = true;
    spec.minex = true;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(2, result.size());
    CHECK_EQ("key1", result[0].field);
    CHECK_EQ("key2", result[1].field);

    // inf and revered
    spec.minex = false;
    spec.maxex = false;
    spec.min = "-";
    spec.max = "+";
    spec.max_infinite = true;
    spec.reversed = true;
    s = hash_->RangeByLex(key_, spec, &result);
    CHECK(s.ok());
    CHECK_EQ(4 + 26, result.size());
    CHECK_EQ("key3", result[0].field);
    CHECK_EQ("key2", result[1].field);
    CHECK_EQ("key1", result[2].field);
    CHECK_EQ("key0", result[3].field);
    hash_->Del(key_);
}

TEST_CASE_FIXTURE(RedisHashTest, "HRangeByLexNonExistingKey") {
    std::vector<titandb::FieldValue> result;
    titandb::RangeLexSpec spec;
    spec.offset = 0;
    spec.count = INT_MAX;
    spec.min = "any-start-key";
    spec.max = "any-end-key";
    auto s = hash_->RangeByLex("non-existing-key", spec, &result);
    CHECK(s.ok());
    CHECK_EQ(result.size(), 0);
}
