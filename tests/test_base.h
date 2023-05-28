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
#ifndef KVROCKS_TEST_BASE_H
#define KVROCKS_TEST_BASE_H

#include <gtest/gtest.h>

#include "turbo/files/filesystem.h"

#include "titandb/storage/redis_db.h"
#include "titandb/types/redis_hash.h"
#include "turbo/log/logging.h"

class TestBase : public testing::Test {  // NOLINT
protected:
    explicit TestBase() : config_(new titandb::Config()) {
        config_->db_dir = "testdb";
        config_->backup_dir = "testdb/backup";
        config_->rocks_db.compression = rocksdb::CompressionType::kNoCompression;
        config_->rocks_db.write_buffer_size = 1;
        config_->rocks_db.block_size = 100;
        storage_ = new titandb::Storage(config_);
        auto s = storage_->Open();
        if (!s.ok()) {
            std::cout << "Failed to open the storage, encounter error: " << s.message() << std::endl;
            assert(s.ok());
        }
    }

    ~TestBase() override {
        auto db_dir = config_->db_dir;
        delete storage_;
        delete config_;

        std::error_code ec;
        TURBO_LOG(INFO)<<"db_dir: "<<db_dir;
        turbo::filesystem::remove_all(db_dir, ec);
        if (ec) {
            std::cout << "Encounter filesystem error: " << ec << std::endl;
        }
    }

    titandb::Storage *storage_;
    titandb::Config *config_ = nullptr;
    std::string key_;
    std::vector<std::string_view> fields_;
    std::vector<std::string_view> values_;
};

#endif  // KVROCKS_TEST_BASE_H
