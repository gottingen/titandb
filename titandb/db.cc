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

#include "titandb/db.h"
#include "turbo/format/str_format.h"

namespace titandb {
    TitanDB::TitanDB(Storage *s, const std::string_view &ns) : _storage(s), _ns(ns.data(), ns.size()),
                                                               _config(_storage->GetConfig()) {
        _strings_db = std::make_unique<RedisString>(_storage, _ns);
        _bitmap_db = std::make_unique<RedisBitmap>(_storage, _ns);
        _hash_db = std::make_unique<RedisHash>(_storage, _ns);
        _key_db = std::make_unique<RedisDB>(_storage, _ns);
        _list_db = std::make_unique<RedisList>(_storage, _ns);
        _set_db = std::make_unique<RedisSet>(_storage, _ns);
        _geo_db = std::make_unique<RedisGeo>(_storage, _ns);
        _sorted_int_db = std::make_unique<RedisSortedInt>(_storage, _ns);
        _zset_db = std::make_unique<RedisZSet>(_storage, _ns);

    }

    turbo::ResultStatus<Storage *> TitanDB::CreateStorage(Config *config) {
        auto *storage = new Storage(config);
        auto s = storage->Open();
        if (!s.ok()) {
            std::cout << "Failed to open the storage, encounter error: " << s.message() << std::endl;
            return turbo::UnavailableError(turbo::Format("Failed to open the storage, encounter error: {}", s.message()));
        }
        return storage;
    }

    turbo::Status TitanDB::FlushAll() {
        auto s = _key_db->FlushAll();
        return s.ok() ? turbo::OkStatus() : turbo::UnavailableError(s.ToString());
    }

    turbo::Status TitanDB::FlushDB() {
        auto s = _key_db->FlushAll();
        return s.ok() ? turbo::OkStatus() : turbo::UnavailableError(s.ToString());
    }

    turbo::Status TitanDB::BeginTxn() {
        return _storage->BeginTxn();
    }

    turbo::Status TitanDB::CommitTxn() {
        return _storage->CommitTxn();
    }

    void TitanDB::FlushBackUp(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
        _storage->PurgeOldBackups(num_backups_to_keep, backup_max_keep_hours);
    }

    turbo::Status TitanDB::BGSave() {
        auto s = _storage->CreateBackup();
        return s.ok() ? turbo::OkStatus() : turbo::UnavailableError(s.ToString());
    }

    turbo::Status TitanDB::Compact(const std::string_view &ns) {
        std::string begin_key;
        std::string end_key;
        if (ns != kDefaultNamespace) {
            std::string prefix;
            ComposeNamespaceKey(ns, "", &prefix, false);
            auto s = _key_db->FindKeyRangeWithPrefix(prefix, std::string(), &begin_key, &end_key);
            if (!s.ok()) {
                if (s.IsNotFound()) {
                    return turbo::OkStatus();
                }

                return turbo::UnavailableError( s.ToString());
            }
        }

        auto s = _storage->Compact(nullptr, nullptr);
        return s.ok() ? turbo::OkStatus() : turbo::UnavailableError(s.ToString());
    }
}  // namespace titandb
