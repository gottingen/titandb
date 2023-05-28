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

#include "titandb/tedis.h"

using namespace tedis;

int main() {
    tedis::Tedis db;
    TedisOptions tds_options;
    tds_options.options.create_if_missing = true;
    tedis::Status s = db.Open(tds_options, "./db");
    if (s.ok()) {
        printf("Open success\n");
    } else {
        printf("Open failed, error: %s\n", s.ToString().c_str());
        return -1;
    }

    int32_t ret;
    // Set
    s = db.Set("TEST_KEY", "TEST_VALUE");
    printf("Set return: %s\n", s.ToString().c_str());

    // Get
    std::string value;
    s = db.Get("TEST_KEY", &value);
    printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

    // SetBit
    s = db.SetBit("SETBIT_KEY", 7, 1, &ret);
    printf("SetBit return: %s, ret: %d\n",
           s.ToString().c_str(), ret);

    // GetSet
    s = db.GetSet("TEST_KEY", "Hello", &value);
    printf("GetSet return: %s, old_value: %s",
           s.ToString().c_str(), value.c_str());

    // SetBit
    s = db.SetBit("SETBIT_KEY", 7, 1, &ret);
    printf("Setbit return: %s\n", s.ToString().c_str());

    // GetBit
    s = db.GetBit("SETBIT_KEY", 7, &ret);
    printf("GetBit return: %s, ret: %d\n",
           s.ToString().c_str(), ret);

    // MSet
    std::vector<tedis::KeyValue> kvs;
    kvs.push_back({"TEST_KEY1", "TEST_VALUE1"});
    kvs.push_back({"TEST_KEY2", "TEST_VALUE2"});
    s = db.MSet(kvs);
    printf("MSet return: %s\n", s.ToString().c_str());

    // MGet
    std::vector<tedis::ValueStatus> vss;
    std::vector<std::string> keys{"TEST_KEY1",
                                  "TEST_KEY2", "TEST_KEY_NOT_EXIST"};
    s = db.MGet(keys, &vss);
    printf("MGet return: %s\n", s.ToString().c_str());
    for (size_t idx = 0; idx != keys.size(); idx++) {
        printf("idx = %d, keys = %s, value = %s\n",
               idx, keys[idx].c_str(), vss[idx].value.c_str());
    }

    // Setnx
    s = db.Setnx("TEST_KEY", "TEST_VALUE", &ret);
    printf("Setnx return: %s, value: %s, ret: %d\n",
           s.ToString().c_str(), value.c_str(), ret);

    // MSetnx
    s = db.MSetnx(kvs, &ret);
    printf("MSetnx return: %s, ret: %d\n", s.ToString().c_str(), ret);

    // Setrange
    s = db.Setrange("TEST_KEY", 10, "APPEND_VALUE", &ret);
    printf("Setrange return: %s, ret: %d\n", s.ToString().c_str(), ret);

    // Getrange
    s = db.Getrange("TEST_KEY", 0, -1, &value);
    printf("Getrange return: %s, value: %s\n",
           s.ToString().c_str(), value.c_str());

    // Append
    std::string append_value;
    s = db.Set("TEST_KEY", "TEST_VALUE");
    s = db.Append("TEST_KEY", "APPEND_VALUE", &ret);
    s = db.Get("TEST_KEY", &append_value);
    printf("Append return: %s, value: %s, ret: %d\n",
           s.ToString().c_str(), append_value.c_str(), ret);

    // BitCount
    s = db.BitCount("TEST_KEY", 0, -1, &ret, false);
    printf("BitCount return: %s, ret: %d\n", s.ToString().c_str(), ret);

    // BitCount
    s = db.BitCount("TEST_KEY", 0, -1, &ret, true);
    printf("BitCount return: %s, ret: %d\n", s.ToString().c_str(), ret);

    // BitOp
    int64_t bitop_ret;
    s = db.Set("BITOP_KEY1", "FOOBAR");
    s = db.Set("BITOP_KEY2", "ABCDEF");
    s = db.Set("BITOP_KEY3", "TEDIS");
    std::vector<std::string> src_keys{"BITOP_KEY1", "BITOP_KEY2", "BITOP_KEY3"};
    // and
    s = db.BitOp(tedis::BitOpType::kBitOpAnd,
                 "BITOP_DESTKEY", src_keys, &bitop_ret);
    printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);
    // or
    s = db.BitOp(tedis::BitOpType::kBitOpOr,
                 "BITOP_DESTKEY", src_keys, &bitop_ret);
    printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);
    // xor
    s = db.BitOp(tedis::BitOpType::kBitOpXor,
                 "BITOP_DESTKEY", src_keys, &bitop_ret);
    printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);
    // not
    std::vector<std::string> not_keys{"BITOP_KEY1"};
    s = db.BitOp(tedis::BitOpType::kBitOpNot,
                 "BITOP_DESTKEY", not_keys, &bitop_ret);
    printf("BitOp return: %s, ret: %d\n", s.ToString().c_str(), bitop_ret);

    // BitPos
    int64_t bitpos_ret;
    s = db.Set("BITPOS_KEY", "\xff\x00\x00");
    // bitpos key bit
    s = db.BitPos("BITPOS_KEY", 1, &bitpos_ret);
    printf("BitPos return: %s, ret: %d\n", s.ToString().c_str(), bitpos_ret);
    // bitpos key bit [start]
    s = db.BitPos("BITPOS_KEY", 1, 0, &bitpos_ret);
    printf("BitPos return: %s, ret: %d\n", s.ToString().c_str(), bitpos_ret);
    // bitpos key bit [start] [end]
    s = db.BitPos("BITPOS_KEY", 1, 0, 4, &bitpos_ret);
    printf("BitPos return: %s, ret: %d\n", s.ToString().c_str(), bitpos_ret);

    // Decrby
    int64_t decrby_ret;
    s = db.Set("TEST_KEY", "12345");
    s = db.Decrby("TEST_KEY", 5, &decrby_ret);
    printf("Decrby return: %s, ret: %d\n", s.ToString().c_str(), decrby_ret);

    // Incrby
    int64_t incrby_ret;
    s = db.Incrby("INCRBY_KEY", 5, &incrby_ret);
    printf("Incrby return: %s, ret: %d\n", s.ToString().c_str(), incrby_ret);

    // Incrbyfloat
    s = db.Set("INCRBYFLOAT_KEY", "10.50");
    s = db.Incrbyfloat("INCRBYFLOAT_KEY", "0.1", &value);
    printf("Incrbyfloat return: %s, value: %s\n",
           s.ToString().c_str(), value.c_str());

    // Setex
    s = db.Setex("TEST_KEY", "TEST_VALUE", 1);
    printf("Setex return: %s\n", s.ToString().c_str());
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    s = db.Get("TEST_KEY", &value);
    printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

    // Strlen
    s = db.Set("TEST_KEY", "TEST_VALUE");
    int32_t len = 0;
    s = db.Strlen("TEST_KEY", &len);
    printf("Strlen return: %s, strlen: %d\n", s.ToString().c_str(), len);


    // Expire
    std::map<tedis::DataType, Status> key_status;
    s = db.Set("EXPIRE_KEY", "EXPIREVALUE");
    printf("Set return: %s\n", s.ToString().c_str());
    db.Expire("EXPIRE_KEY", 1, &key_status);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    s = db.Get("EXPIRE_KEY", &value);
    printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

    // Compact
    s = db.Compact(tedis::DataType::kStrings);
    printf("Compact return: %s\n", s.ToString().c_str());

    return 0;
}
