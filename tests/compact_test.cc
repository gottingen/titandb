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

#include "turbo/files/filesystem.h"

#include "titandb/storage/redis_metadata.h"
#include "titandb/storage/storage.h"
#include "titandb/types/redis_hash.h"
#include "turbo/log/logging.h"

using namespace titandb;

TEST_CASE("Compact, Filter") {
    Config *config = new Config();
    config->db_dir = "compactdb";
    config->backup_dir = "compactdb/backup";
    config->rocks_db.compression = rocksdb::CompressionType::kNoCompression;
    config->slot_id_encoded = false;

    std::error_code ec;
    if(turbo::filesystem::exists(config->db_dir, ec)) {
        turbo::filesystem::remove_all(config->db_dir, ec);
    }

    auto storage = std::make_unique<Storage>(config);
    auto s = storage->Open();
    REQUIRE(s.ok());

    int ret = 0;
    TLOG_INFO("hash db init");
    std::string ns = "test_compact";
    REQUIRE(storage.get() != nullptr);
    auto hash = std::make_unique<RedisHash>(storage.get(), ns);
    TLOG_INFO("hash db created");
    std::string expired_hash_key = "expire_hash_key";
    std::string live_hash_key = "live_hash_key";
    hash->Set(expired_hash_key, "f1", "v1", &ret);
    hash->Set(expired_hash_key, "f2", "v2", &ret);
    hash->Expire(expired_hash_key, 1);  // expired
    usleep(10000);
    hash->Set(live_hash_key, "f1", "v1", &ret);
    hash->Set(live_hash_key, "f2", "v2", &ret);
    auto status = storage->Compact(nullptr, nullptr);
    assert(status.ok());
    rocksdb::DB *db = storage->GetDB();
    rocksdb::ReadOptions read_options;
    read_options.snapshot = db->GetSnapshot();
    read_options.fill_cache = false;

    auto new_iterator = [db, read_options, &storage](const std::string &name) {
        return std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options, storage->GetCFHandle(name)));
    };

    auto iter = new_iterator("metadata");
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string user_key, user_ns;
        ExtractNamespaceKey(iter->key(), &user_ns, &user_key);
        CHECK_EQ(user_key, live_hash_key);
    }

    iter = new_iterator("subkey");
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        InternalKey ikey(iter->key());
        CHECK_EQ(ikey.GetKey().ToString(), live_hash_key);
    }

    status = storage->Compact(nullptr, nullptr);
    assert(status.ok());

    iter = new_iterator("default");
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        InternalKey ikey(iter->key());
        CHECK_EQ(ikey.GetKey().ToString(), live_hash_key);
    }

    Slice mk_with_ttl = "mk_with_ttl";
    hash->Set(mk_with_ttl, "f1", "v1", &ret);
    hash->Set(mk_with_ttl, "f2", "v2", &ret);

    int retry = 2;
    while (retry-- > 0) {
        status = storage->Compact(nullptr, nullptr);
        assert(status.ok());
        std::vector<FieldValue> fieldvalues;
        auto get_res = hash->GetAll(mk_with_ttl, &fieldvalues);
        auto s_expire = hash->Expire(mk_with_ttl, 1);  // expired immediately..

        if (retry == 1) {
            REQUIRE(get_res.ok());  // not expired first time
            REQUIRE(s_expire.ok());
        } else {
            REQUIRE(get_res.ok());  // expired but still return ok....
            REQUIRE_EQ(0, fieldvalues.size());
            REQUIRE(s_expire.IsNotFound());
        }
        usleep(10000);
    }

    db->ReleaseSnapshot(read_options.snapshot);
    turbo::filesystem::remove_all(config->db_dir, ec);
    if (ec) {
        std::cout << "Encounter filesystem error: " << ec << std::endl;
    }

}
