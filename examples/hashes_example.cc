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
    // HSet
    int32_t res;
    s = db.HSet("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1", &res);
    printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
    s = db.HSet("TEST_KEY1", "TEST_FIELD2", "TEST_VALUE2", &res);
    printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);

    s = db.HSet("TEST_KEY2", "TEST_FIELD1", "TEST_VALUE1", &res);
    printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
    s = db.HSet("TEST_KEY2", "TEST_FIELD2", "TEST_VALUE2", &res);
    printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
    s = db.HSet("TEST_KEY2", "TEST_FIELD3", "TEST_VALUE3", &res);
    printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);

    // HGet
    std::string value;
    s = db.HGet("TEST_KEY1", "TEST_FIELD1", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY1", "TEST_FIELD2", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY1", "TEST_FIELD3", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY_NOT_EXIST", "TEST_FIELD", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

    // HMSet
    std::vector<tedis::FieldValue> fvs;
    fvs.push_back({"TEST_FIELD1", "TEST_VALUE1"});
    fvs.push_back({"TEST_FIELD2", "TEST_VALUE2"});
    s = db.HMSet("TEST_HASH", fvs);
    printf("HMset return: %s\n", s.ToString().c_str());

    // HMGet
    std::vector<tedis::ValueStatus> vss;
    std::vector<std::string> fields;
    fields.push_back("TEST_FIELD1");
    fields.push_back("TEST_FIELD2");
    s = db.HMGet("TEST_HASH", fields, &vss);
    printf("HMget return: %s\n", s.ToString().c_str());
    for (uint32_t idx = 0; idx != fields.size(); idx++) {
        printf("idx = %d, field = %s, value = %s\n",
               idx, fields[idx].c_str(), vss[idx].value.c_str());
    }

    // HLEN
    s = db.HLen("TEST_HASH", &res);
    printf("HLen return : %s, len = %d\n", s.ToString().c_str(), res);

    // Compact
    s = db.Compact(tedis::DataType::kHashes);
    printf("Compact return: %s\n", s.ToString().c_str());

    // Expire
    std::map<tedis::DataType, Status> key_status;
    db.Expire("TEST_KEY1", 1, &key_status);
    printf("Expire return: %s\n", s.ToString().c_str());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    s = db.HGet("TEST_KEY1", "TEST_FIELD1", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY1", "TEST_FIELD2", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

    s = db.HGet("TEST_KEY2", "TEST_FIELD1", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY2", "TEST_FIELD2", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY2", "TEST_FIELD3", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

    // Compact
    s = db.Compact(tedis::DataType::kHashes);
    printf("Compact return: %s\n", s.ToString().c_str());

    s = db.HGet("TEST_KEY2", "TEST_FIELD1", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY2", "TEST_FIELD2", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
    s = db.HGet("TEST_KEY2", "TEST_FIELD3", &value);
    printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

    // Exists
    s = db.HSet("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1", &res);
    printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
    s = db.HExists("TEST_KEY1", "TEST_FIELD1");
    printf("HExists return: %s\n", s.ToString().c_str());

    // HIncrby
    int64_t hincrby_value;
    s = db.HIncrby("TEST_KEY1", "TEST_HINCRBY_FIELD", 100, &hincrby_value);
    printf("HIncrby return: %s, value = %lld\n", s.ToString().c_str(), hincrby_value);
    return 0;
}
