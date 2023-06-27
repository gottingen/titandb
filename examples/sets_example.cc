//
//  Copyright 2023 The titan-search Authors.
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

#include <thread>
#include <memory>
#include "titandb/db.h"
#include "turbo/format/fmt/printf.h"
#include "turbo/files/filesystem.h"

using namespace titandb;

int main() {
    std::unique_ptr<titandb::Config> config(new titandb::Config());
    config->db_dir = "testdb";
    config->rocks_db.compression = rocksdb::CompressionType::kNoCompression;
    config->rocks_db.write_buffer_size = 1;
    config->rocks_db.block_size = 100;
    std::unique_ptr<titandb::Storage> storage(new titandb::Storage(config.get()));
    auto ss = storage->Open();
    if (ss.ok()) {
        printf("Open success\n");
    } else {
        printf("Open failed, error: %s\n", ss.ToString().c_str());
        return -1;
    }
    titandb::TitanDB db(storage.get(), "th");
    // SAdd
    int32_t ret = 0;
    std::vector<std::string_view> members{"MM1", "MM2", "MM3", "MM2"};
    auto sa = db.SAdd("SADD_KEY", members);
    printf("SAdd return: %s, ret = %d\n", sa.status().ToString().c_str(), sa.value());

    // SCard
    ret = 0;
    auto sc = db.SCard("SADD_KEY");
    printf("SCard, return: %s, scard ret = %d\n", sc.status().ToString().c_str(), sc.value());

    return 0;
}
