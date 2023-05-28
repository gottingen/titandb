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
    config->backup_dir = "testdb/backup";
    config->rocks_db.compression = rocksdb::CompressionType::kNoCompression;
    config->rocks_db.write_buffer_size = 1;
    config->rocks_db.block_size = 100;
    std::unique_ptr<titandb::Storage> storage(new titandb::Storage(config.get()));
    auto ss = storage->Open();
    if (ss.ok()) {
        fmt::printf("Open success\n");
    } else {
        fmt::printf("Open failed, error: %s\n", ss.ToString().c_str());
        return -1;
    }
    titandb::TitanDB db(storage.get(), "th");
    // Set
    auto s1 = db.Set("TEST_KEY", "TEST_VALUE");
    fmt::printf("Set return: %s\n", s1.ToString().c_str());

    // Get
    auto s2 = db.Get("TEST_KEY");
    fmt::printf("Get return: %s, value: %s\n", s2.status().ToString().c_str(), s2.value().c_str());

    // SetBit
    auto s3 = db.SetBit("SETBIT_KEY", 7, 1);
    fmt::printf("SetBit return: %s, ret: %d\n",
           s3.status().ToString().c_str(), s3.value());

    // GetSet
    auto s4 = db.GetSet("TEST_KEY", "Hello");
    fmt::printf("GetSet return: %s, old_value: %s",
           s4.status().ToString().c_str(), s4.value().c_str());

    // SetBit
    auto  s5 = db.SetBit("SETBIT_KEY", 7, 1);
    fmt::printf("Setbit return: %s\n", s5.status().ToString().c_str());

    // GetBit
    auto s6 = db.GetBit("SETBIT_KEY", 7);
    fmt::printf("GetBit return: %s, ret: %d\n",
           s6.status().ToString().c_str(), s6.value());

    // MSet
    std::vector<StringPair> kvs;
    kvs.push_back({"TEST_KEY1", "TEST_VALUE1"});
    kvs.push_back({"TEST_KEY2", "TEST_VALUE2"});
    auto s7 = db.MSet(kvs);
    fmt::printf("MSet return: %s\n", s7.status().ToString().c_str());

    // MGet
    std::vector<std::string_view> keys{"TEST_KEY1",
                                  "TEST_KEY2", "TEST_KEY_NOT_EXIST"};
    auto s8 = db.MGet(keys);
    for (size_t idx = 0; idx != keys.size(); idx++) {
        fmt::printf("idx = %d, keys = %s, value = %s\n",
               idx, keys[idx], s8[idx].ok()? s8[idx].value() : "not found");
    }

    // Setnx
    auto s9 = db.SetNx("TEST_KEY", "TEST_VALUE");
    fmt::printf("Setnx return: %s,  ret: %d\n",
           s9.status().ToString().c_str(), s9.value_or(-1));

    // MSetnx
    auto s10 = db.MSetNx(kvs);
    fmt::printf("MSetnx return: %s, ret: %d\n", s10.status().ToString().c_str(), s10.value_or(-1));

    // Setrange
    auto s11 = db.SetRange("TEST_KEY", 10, "APPEND_VALUE");
    fmt::printf("Setrange return: %s, ret: %d\n", s11.status().ToString().c_str(), s11.value_or(-1));

    // Getrange
    auto s12 = db.GetRange("TEST_KEY", 0, -1);
    fmt::printf("Getrange return: %s, value: %s\n",
           s12.status().ToString().c_str(), s12.value_or("not found"));

    // Append
    std::string append_value;
    auto s13 = db.Set("TEST_KEY", "TEST_VALUE");
    auto s14 = db.Append("TEST_KEY", "APPEND_VALUE");
    auto s15 = db.Get("TEST_KEY");
    fmt::printf("Append return: %s, value: %s, ret: %d\n",
           s15.status().ToString().c_str(), append_value.c_str(), s14.value_or(-1));

    // BitCount
    auto s16 = db.BitCount("TEST_KEY", 0, -1);
    fmt::printf("BitCount return: %s, ret: %d\n", s16.status().ToString().c_str(), s16.value_or(-1));

    // BitOp
    int64_t bitop_ret;
    auto s17 = db.Set("BITOP_KEY1", "FOOBAR");
    auto s18 = db.Set("BITOP_KEY2", "ABCDEF");
    auto s19 = db.Set("BITOP_KEY3", "TEDIS");
    std::vector<std::string_view> src_keys{"BITOP_KEY1", "BITOP_KEY2", "BITOP_KEY3"};
    // Decrby
    auto s20 = db.Set("TEST_KEY", "12345");
    auto s21 = db.DecrBy("TEST_KEY", 5);
    fmt::printf("Decrby return: %s, ret: %d\n", s21.status().ToString().c_str(), s21.value_or(-1));

    // Incrby
    auto s22 = db.IncrBy("INCRBY_KEY", 5);
    fmt::printf("Incrby return: %s, ret: %d\n", s22.status().ToString().c_str(), s22.value_or(-1));

    // Incrbyfloat
    auto s23 = db.Set("INCRBYFLOAT_KEY", "10.50");
    auto s24 = db.IncrByFloat("INCRBYFLOAT_KEY", 0.1);
    fmt::printf("Incrbyfloat return: %s, value: %f\n",
           s24.status().ToString().c_str(), s24.value_or(-1.0));

    // Setex
    auto s25 = db.SetEx("TEST_KEY", "TEST_VALUE", 1);
    fmt::printf("Setex return: %s\n", s25.ToString().c_str());
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    auto s26 = db.Get("TEST_KEY");
    fmt::printf("Get return: %s, value: %s\n", s26.status().ToString().c_str(), s26.value_or("nf"));

    // Strlen
    auto s27 = db.Set("TEST_KEY", "TEST_VALUE");
    int32_t len = 0;
    auto s28 = db.Strlen("TEST_KEY");
    fmt::printf("Strlen return: %s, strlen: %d\n", s28.status().ToString().c_str(), s28.value_or(-1));


    // Expire
    auto s29 = db.Set("EXPIRE_KEY", "EXPIREVALUE");
    fmt::printf("Set return: %s\n", s29.ToString().c_str());
    auto s100 = db.Expire("EXPIRE_KEY", 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    auto s30 = db.Get("EXPIRE_KEY");
    fmt::printf("Get return: %s, value: %s\n", s30.status().ToString().c_str(), s30.value_or("expired").c_str());

    // Compact
    auto s31 = db.Compact("th");
    fmt::printf("Compact return: %s\n", s31.ToString().c_str());

    return 0;
}
