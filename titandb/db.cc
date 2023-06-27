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
        _db = std::make_unique<RedisDB>(_storage, _ns);

    }

    turbo::Status TitanDB::init(Storage *s, const std::string_view &ns) {
        _storage = s;
        _ns = ns;
        _config = _storage->GetConfig();
        _db = std::make_unique<RedisDB>(_storage, _ns);
        return _db?turbo::OkStatus():turbo::UnauthenticatedError("db new error");
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

    turbo::Status TitanDB::BeginTxn() {
        return _storage->BeginTxn();
    }

    turbo::Status TitanDB::CommitTxn() {
        return _storage->CommitTxn();
    }

}  // namespace titandb
