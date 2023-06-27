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
#include "titandb/types/redis_list.h"

using namespace titandb;

class RedisListTest : public TestBase {
protected:
    explicit RedisListTest() { list_ = std::make_unique<titandb::RedisList>(storage_, "list_ns");  key_ = "test-list-key";
        fields_ = {"list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
                   "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
                   "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
                   "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5"};}

    ~RedisListTest() override = default;
    

    std::unique_ptr<titandb::RedisList> list_;
};

class RedisListSpecificTest : public RedisListTest {
public:
    RedisListSpecificTest() {
        key_ = "test-list-specific-key";
        fields_ = {"0", "1", "2", "3", "4", "3", "6", "7", "3", "8", "9", "3", "9", "3", "9"};
    }
};

class RedisListLMoveTest : public RedisListTest {
public:
    RedisListLMoveTest() {
        list_->Del(key_);
        list_->Del(dst_key_);
        fields_ = {"src1", "src2", "src3", "src4"};
        dst_fields_ = {"dst", "dst2", "dst3", "dst4"};
    }
    ~RedisListLMoveTest() override {
        list_->Del(key_);
        list_->Del(dst_key_);
    }
protected:
    void listElementsAreEqualTo(const Slice &key, int start, int stop, const std::vector<Slice> &expected_elems) {
        std::vector<std::string> actual_elems;
        auto s = list_->Range(key, start, stop, &actual_elems);
        CHECK(s.ok());

        CHECK_EQ(actual_elems.size(), expected_elems.size());

        for (size_t i = 0; i < actual_elems.size(); ++i) {
            CHECK_EQ(actual_elems[i], expected_elems[i].ToString());
        }
    }

    std::string dst_key_ = "test-dst-key";
    std::vector<std::string_view> dst_fields_;
};

TEST_CASE_FIXTURE(RedisListTest, "PushAndPop") {
    int ret = 0;
    list_->Push(key_, fields_, true, &ret);
    CHECK_EQ(fields_.size(), ret);
    for (auto &field: fields_) {
        std::string elem;
        list_->Pop(key_, false, &elem);
        CHECK_EQ(elem, field);
    }
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    for (auto &field: fields_) {
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "Pushx") {
    int ret = 0;
    Slice pushx_key("test-pushx-key");
    rocksdb::Status s = list_->PushX(pushx_key, fields_, true, &ret);
    CHECK(s.ok());
    list_->Push(pushx_key, fields_, true, &ret);
    CHECK_EQ(fields_.size(), ret);
    s = list_->PushX(pushx_key, fields_, true, &ret);
    CHECK(s.ok());
    CHECK_EQ(ret, fields_.size() * 2);
    list_->Del(pushx_key);
}

TEST_CASE_FIXTURE(RedisListTest, "Index") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::string elem;
    for (size_t i = 0; i < fields_.size(); i++) {
        list_->Index(key_, static_cast<int>(i), &elem);
        CHECK_EQ(fields_[i], elem);
    }
    for (auto &field: fields_) {
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    rocksdb::Status s = list_->Index(key_, -1, &elem);
    CHECK(s.IsNotFound());
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "Set") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    Slice new_elem("new_elem");
    list_->Set(key_, -1, new_elem);
    std::string elem;
    list_->Index(key_, -1, &elem);
    CHECK_EQ(new_elem.ToString(), elem);
    for (size_t i = 0; i < fields_.size(); i++) {
        list_->Pop(key_, true, &elem);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "Range") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::vector<std::string> elems;
    list_->Range(key_, 0, int(elems.size() - 1), &elems);
    CHECK_EQ(elems.size(), fields_.size());
    for (size_t i = 0; i < elems.size(); i++) {
        CHECK_EQ(fields_[i], elems[i]);
    }
    for (auto &field: fields_) {
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "Rem") {
    int ret = 0;
    uint32_t len = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    Slice del_elem("list-test-key-1");
    // lrem key_ 1 list-test-key-1
    list_->Rem(key_, 1, del_elem, &ret);
    CHECK_EQ(1, ret);
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 1, len);
    for (size_t i = 1; i < fields_.size(); i++) {
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, fields_[i]);
    }
    // lrem key_ 0 list-test-key-1
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Rem(key_, 0, del_elem, &ret);
    CHECK_EQ(4, ret);
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 4, len);
    for (auto &field: fields_) {
        std::string elem;
        if (field == del_elem) continue;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    // lrem key_ 1 nosuchelement
    Slice no_elem("no_such_element");
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Rem(key_, 1, no_elem, &ret);
    CHECK_EQ(0, ret);
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size(), len);
    for (auto &field: fields_) {
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    // lrem key_ -1 list-test-key-1
    list_->Push(key_, fields_, false, &ret);
    list_->Rem(key_, -1, del_elem, &ret);
    CHECK_EQ(1, ret);
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 1, len);
    int cnt = 0;
    for (auto &field: fields_) {
        std::string elem;
        if (field == del_elem) {
            if (++cnt > 3) continue;
        }
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    // lrem key_ -5 list-test-key-1
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Rem(key_, -5, del_elem, &ret);
    CHECK_EQ(4, ret);
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 4, len);
    for (auto &field: fields_) {
        std::string elem;
        if (field == del_elem) continue;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListSpecificTest, "Rem") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    Slice del_elem("9");
    // lrem key_ 1 9
    list_->Rem(key_, 1, del_elem, &ret);
    CHECK_EQ(1, ret);
    uint32_t len = 0;
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 1, len);
    int cnt = 0;
    for (auto &field: fields_) {
        if (field == del_elem) {
            if (++cnt <= 1) continue;
        }
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    // lrem key_ -2 9
    list_->Push(key_, fields_, false, &ret);
    list_->Rem(key_, -2, del_elem, &ret);
    CHECK_EQ(2, ret);
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 2, len);
    cnt = 0;
    for (size_t i = fields_.size(); i > 0; i--) {
        if (fields_[i - 1] == del_elem) {
            if (++cnt <= 2) continue;
        }
        std::string elem;
        list_->Pop(key_, false, &elem);
        CHECK_EQ(elem, fields_[i - 1]);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "Trim") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Trim(key_, 1, 2000);
    uint32_t len = 0;
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 1, len);
    for (size_t i = 1; i < fields_.size(); i++) {
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, fields_[i]);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListSpecificTest, "Trim") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    // ltrim key_ 3 -3 then linsert 2 3 and lrem key_ 5 3
    Slice del_elem("3");
    list_->Trim(key_, 3, -3);
    uint32_t len = 0;
    list_->Size(key_, &len);
    CHECK_EQ(fields_.size() - 5, len);
    Slice insert_elem("3");
    list_->Insert(key_, Slice("2"), insert_elem, true, &ret);
    CHECK_EQ(-1, ret);
    list_->Rem(key_, 5, del_elem, &ret);
    CHECK_EQ(4, ret);
    for (size_t i = 3; i < fields_.size() - 2; i++) {
        if (fields_[i] == del_elem) continue;
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, fields_[i]);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "RPopLPush") {
    int ret = 0;
    list_->Push(key_, fields_, true, &ret);
    CHECK_EQ(fields_.size(), ret);
    Slice dst("test-list-rpoplpush-key");
    for (auto &field: fields_) {
        std::string elem;
        list_->RPopLPush(key_, dst, &elem);
        CHECK_EQ(field, elem);
    }
    for (auto &field: fields_) {
        std::string elem;
        list_->Pop(dst, false, &elem);
        CHECK_EQ(elem, field);
    }
    list_->Del(key_);
    list_->Del(dst);
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcNotExist") {
    std::string elem;
    auto s = list_->LMove(key_, dst_key_, true, true, &elem);
    CHECK_EQ(elem, "");
    CHECK_FALSE(s.ok());
    CHECK(s.IsNotFound());
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcAndDstAreTheSameSingleElem") {
    int ret = 0;
    std::string_view element = fields_[0];
    list_->Push(key_, {element}, false, &ret);
    CHECK_EQ(1, ret);
    std::string expected_elem;
    auto s = list_->LMove(key_, key_, true, true, &expected_elem);
    CHECK_EQ(expected_elem, element);
    CHECK(s.ok());
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[0]});
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcAndDstAreTheSameManyElemsLeftRight") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::string elem;
    auto s = list_->LMove(key_, key_, true, false, &elem);
    CHECK(s.ok());
    CHECK_EQ(elem, fields_[0]);
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size() + 1),
                           {fields_[1], fields_[2], fields_[3], fields_[0]});
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcAndDstAreTheSameManyElemsRightLeft") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::string elem;
    auto s = list_->LMove(key_, key_, false, true, &elem);
    CHECK(s.ok());
    CHECK_EQ(elem, fields_[fields_.size() - 1]);
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size() + 1),
                           {fields_[3], fields_[0], fields_[1], fields_[2]});
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveDstNotExist") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::string elem;
    auto s = list_->LMove(key_, dst_key_, true, false, &elem);
    CHECK_EQ(elem, fields_[0]);
    CHECK(s.ok());
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[1], fields_[2], fields_[3]});
    listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size()), {fields_[0]});
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcLeftDstLeft") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Push(dst_key_, dst_fields_, false, &ret);
    CHECK_EQ(dst_fields_.size(), ret);
    std::string elem;
    auto s = list_->LMove(key_, dst_key_, true, true, &elem);
    CHECK_EQ(elem, fields_[0]);
    CHECK(s.ok());
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[1], fields_[2], fields_[3]});
    listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                           {fields_[0], dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3]});
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcLeftDstRight") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Push(dst_key_, dst_fields_, false, &ret);
    CHECK_EQ(dst_fields_.size(), ret);
    std::string elem;
    auto s = list_->LMove(key_, dst_key_, true, false, &elem);
    CHECK_EQ(elem, fields_[0]);
    CHECK(s.ok());
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[1], fields_[2], fields_[3]});
    listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                           {dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3], fields_[0]});
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcRightDstLeft") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Push(dst_key_, dst_fields_, false, &ret);
    CHECK_EQ(dst_fields_.size(), ret);
    std::string elem;
    auto s = list_->LMove(key_, dst_key_, false, true, &elem);
    CHECK_EQ(elem, fields_[3]);
    CHECK(s.ok());
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[0], fields_[1], fields_[2]});
    listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                           {fields_[3], dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3]});
}

TEST_CASE_FIXTURE(RedisListLMoveTest, "LMoveSrcRightDstRight") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    list_->Push(dst_key_, dst_fields_, false, &ret);
    CHECK_EQ(dst_fields_.size(), ret);
    std::string elem;
    auto s = list_->LMove(key_, dst_key_, false, false, &elem);
    CHECK_EQ(elem, fields_[3]);
    CHECK(s.ok());
    listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[0], fields_[1], fields_[2]});
    listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                           {dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3], fields_[3]});
}

TEST_CASE_FIXTURE(RedisListTest, "LPopEmptyList") {
    std::string non_existing_key{"non-existing-key"};
    list_->Del(non_existing_key);
    std::string elem;
    auto s = list_->Pop(non_existing_key, true, &elem);
    CHECK(s.IsNotFound());
    std::vector<std::string> elems;
    s = list_->PopMulti(non_existing_key, true, 10, &elems);
    CHECK(s.IsNotFound());
}

TEST_CASE_FIXTURE(RedisListTest, "LPopOneElement") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    for (auto &field: fields_) {
        std::string elem;
        list_->Pop(key_, true, &elem);
        CHECK_EQ(elem, field);
    }
    std::string elem;
    auto s = list_->Pop(key_, true, &elem);
    CHECK(s.IsNotFound());
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "LPopMulti") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::vector<std::string> elems;
    size_t requested_size = fields_.size() / 3;
    auto s = list_->PopMulti(key_, true, requested_size, &elems);
    CHECK(s.ok());
    CHECK_EQ(elems.size(), requested_size);
    for (size_t i = 0; i < elems.size(); ++i) {
        CHECK_EQ(elems[i], fields_[i]);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "LPopMultiCountGreaterThanListSize") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::vector<std::string> elems;
    auto s = list_->PopMulti(key_, true, 2 * ret, &elems);
    CHECK(s.ok());
    CHECK_EQ(elems.size(), ret);
    for (size_t i = 0; i < elems.size(); ++i) {
        CHECK_EQ(elems[i], fields_[i]);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "RPopEmptyList") {
    std::string non_existing_key{"non-existing-key"};
    list_->Del(non_existing_key);
    std::string elem;
    auto s = list_->Pop(non_existing_key, false, &elem);
    CHECK(s.IsNotFound());
    std::vector<std::string> elems;
    s = list_->PopMulti(non_existing_key, false, 10, &elems);
    CHECK(s.IsNotFound());
}

TEST_CASE_FIXTURE(RedisListTest, "RPopOneElement") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    for (size_t i = 0; i < fields_.size(); i++) {
        std::string elem;
        list_->Pop(key_, false, &elem);
        CHECK_EQ(elem, fields_[fields_.size() - i - 1]);
    }
    std::string elem;
    auto s = list_->Pop(key_, false, &elem);
    CHECK(s.IsNotFound());
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "RPopMulti") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::vector<std::string> elems;
    size_t requested_size = fields_.size() / 3;
    auto s = list_->PopMulti(key_, false, requested_size, &elems);
    CHECK(s.ok());
    CHECK_EQ(elems.size(), requested_size);
    for (size_t i = 0; i < elems.size(); ++i) {
        CHECK_EQ(elems[i], fields_[fields_.size() - i - 1]);
    }
    list_->Del(key_);
}

TEST_CASE_FIXTURE(RedisListTest, "RPopMultiCountGreaterThanListSize") {
    int ret = 0;
    list_->Push(key_, fields_, false, &ret);
    CHECK_EQ(fields_.size(), ret);
    std::vector<std::string> elems;
    auto s = list_->PopMulti(key_, false, 2 * ret, &elems);
    CHECK(s.ok());
    CHECK_EQ(elems.size(), ret);
    for (size_t i = 0; i < elems.size(); ++i) {
        CHECK_EQ(elems[i], fields_[fields_.size() - i - 1]);
    }
    list_->Del(key_);
}
