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

#include "titandb/storage/redis_metadata.h"
#include "test_base.h"
#include "turbo/times/clock.h"
#include "titandb/redis_db.h"

using namespace titandb;

TEST_CASE("InternalKey, EncodeAndDecode") {
    Slice key = "test-metadata-key";
    Slice sub_key = "test-metadata-sub-key";
    Slice ns = "namespace";
    uint64_t version = 12;
    std::string ns_key;

    ComposeNamespaceKey(ns, key, &ns_key);
    InternalKey ikey(ns_key, sub_key, version);
    REQUIRE_EQ(ikey.GetKey(), key);
    REQUIRE_EQ(ikey.GetSubKey(), sub_key);
    REQUIRE_EQ(ikey.GetVersion(), version);
    std::string bytes;
    ikey.Encode(&bytes);
    InternalKey ikey1(bytes);
    CHECK_EQ(ikey, ikey1);
}

TEST_CASE("Metadata, EncodeAndDeocde") {
    std::string string_bytes;
    Metadata string_md(kRedisString);
    string_md.expire = 123000;
    string_md.Encode(&string_bytes);
    Metadata string_md1(kRedisNone);
    string_md1.Decode(string_bytes);
    REQUIRE_EQ(string_md, string_md1);
    ListMetadata list_md;
    list_md.flags = 13;
    list_md.expire = 123000;
    list_md.version = 2;
    list_md.size = 1234;
    list_md.head = 123;
    list_md.tail = 321;
    ListMetadata list_md1;
    std::string list_bytes;
    list_md.Encode(&list_bytes);
    list_md1.Decode(list_bytes);
    REQUIRE_EQ(list_md, list_md1);
}

class RedisTypeTest : public TestBase {
public:
    RedisTypeTest() {
        redis_ = std::make_unique<titandb::RedisDB>(storage_, "default_ns");
        hash_ = std::make_unique<titandb::RedisDB>(storage_, "default_ns");
        key_ = "test-redis-type";
        fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
        values_ = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
    }

    ~RedisTypeTest() override = default;

protected:
    std::unique_ptr<titandb::RedisDB> redis_;
    std::unique_ptr<titandb::RedisDB> hash_;
};

TEST_CASE_FIXTURE(RedisTypeTest, "GetMetadata") {
    int ret = 0;
    std::vector<FieldValue> fvs;
    for (size_t i = 0; i < fields_.size(); i++) {
        fvs.emplace_back(std::string(fields_[i]), std::string(values_[i]));
    }
    rocksdb::Status s = hash_->MSet(key_, fvs, false, &ret);
    CHECK((s.ok() && static_cast<int>(fvs.size()) == ret));
    HashMetadata metadata;
    std::string ns_key;
    redis_->AppendNamespacePrefix(key_, &ns_key);
    redis_->GetMetadata(kRedisHash, ns_key, &metadata);
    CHECK_EQ(fvs.size(), metadata.size);
    s = redis_->Del(key_);
    CHECK(s.ok());
}

TEST_CASE_FIXTURE(RedisTypeTest, "Expire") {
    int ret = 0;
    std::vector<FieldValue> fvs;
    for (size_t i = 0; i < fields_.size(); i++) {
        fvs.emplace_back(std::string(fields_[i]), std::string(values_[i]));
    }
    rocksdb::Status s = hash_->MSet(key_, fvs, false, &ret);
    CHECK((s.ok() && static_cast<int>(fvs.size()) == ret));
    int64_t now = 0;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    redis_->Expire(key_, now * 1000 + 2000);
    int64_t ttl = 0;
    redis_->TTL(key_, &ttl);
    REQUIRE_GT(ttl, 0);
    REQUIRE_LE(ttl, 2000);
    redis_->Del(key_);
}

TEST_CASE("Metadata, MetadataDecodingBackwardCompatibleSimpleKey") {
    auto expire_at = (turbo::ToTimeT(turbo::Now()) + 10) * 1000;
    Metadata md_old(kRedisString, true, false);
    CHECK_FALSE(md_old.Is64BitEncoded());
    md_old.expire = expire_at;
    std::string encoded_bytes;
    md_old.Encode(&encoded_bytes);
    CHECK_EQ(encoded_bytes.size(), 5);

    Metadata md_new(kRedisNone, false, true);  // decoding existing metadata with 64-bit feature activated
    md_new.Decode(encoded_bytes);
    CHECK_FALSE(md_new.Is64BitEncoded());
    CHECK_EQ(md_new.Type(), kRedisString);
    CHECK_EQ(md_new.expire, expire_at);
}

TEST_CASE("Metadata, MetadataDecoding64BitSimpleKey") {
    auto expire_at = (turbo::ToTimeT(turbo::Now()) + 10) * 1000;
    Metadata md_old(kRedisString, true, true);
    CHECK(md_old.Is64BitEncoded());
    md_old.expire = expire_at;
    std::string encoded_bytes;
    md_old.Encode(&encoded_bytes);
    CHECK_EQ(encoded_bytes.size(), 9);
}

TEST_CASE("Metadata, MetadataDecodingBackwardCompatibleComplexKey") {
    auto expire_at = (turbo::ToTimeT(turbo::Now()) + 100) * 1000;
    uint32_t size = 1000000000;
    Metadata md_old(kRedisHash, true, false);
    CHECK_FALSE(md_old.Is64BitEncoded());
    md_old.expire = expire_at;
    md_old.size = size;
    std::string encoded_bytes;
    md_old.Encode(&encoded_bytes);

    Metadata md_new(kRedisHash, false, true);
    md_new.Decode(encoded_bytes);
    CHECK_FALSE(md_new.Is64BitEncoded());
    CHECK_EQ(md_new.Type(), kRedisHash);
    CHECK_EQ(md_new.expire, expire_at);
    CHECK_EQ(md_new.size, size);
}

TEST_CASE("Metadata, Metadata64bitExpiration") {
    auto expire_at = turbo::ToUnixMillis(turbo::Now()) + 1000;
    Metadata md_src(kRedisString, true, true);
    CHECK(md_src.Is64BitEncoded());
    md_src.expire = expire_at;
    std::string encoded_bytes;
    md_src.Encode(&encoded_bytes);

    Metadata md_decoded(kRedisNone, false, true);
    md_decoded.Decode(encoded_bytes);
    CHECK(md_decoded.Is64BitEncoded());
    CHECK_EQ(md_decoded.Type(), kRedisString);
    CHECK_EQ(md_decoded.expire, expire_at);
}

TEST_CASE("Metadata, Metadata64bitSize") {
    uint64_t big_size = 100000000000;
    Metadata md_src(kRedisHash, true, true);
    CHECK(md_src.Is64BitEncoded());
    md_src.size = big_size;
    std::string encoded_bytes;
    md_src.Encode(&encoded_bytes);

    Metadata md_decoded(kRedisNone, false, true);
    md_decoded.Decode(encoded_bytes);
    CHECK(md_decoded.Is64BitEncoded());
    CHECK_EQ(md_decoded.Type(), kRedisHash);
    CHECK_EQ(md_decoded.size, big_size);
}
