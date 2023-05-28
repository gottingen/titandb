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
    // SAdd
    int32_t ret = 0;
    std::vector<std::string> members{"MM1", "MM2", "MM3", "MM2"};
    s = db.SAdd("SADD_KEY", members, &ret);
    printf("SAdd return: %s, ret = %d\n", s.ToString().c_str(), ret);

    // SCard
    ret = 0;
    s = db.SCard("SADD_KEY", &ret);
    printf("SCard, return: %s, scard ret = %d\n", s.ToString().c_str(), ret);

    return 0;
}
