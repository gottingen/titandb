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

#include "titandb/common/encoding.h"

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest/doctest.h"
#include <rocksdb/slice.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

TEST_CASE("Util, EncodeAndDecodeDouble") {
    std::vector<double> values = {-1234, -100.1234, -1.2345, 0, 1.2345, 100.1234, 1234};
    std::string prev_bytes;
    for (auto value: values) {
        std::string bytes;
        PutDouble(&bytes, value);
        double got = DecodeDouble(bytes.data());
        if (!prev_bytes.empty()) {
            REQUIRE_LT(prev_bytes, bytes);
        }
        prev_bytes.assign(bytes);
        REQUIRE_EQ(value, got);
    }
}

TEST_CASE("Util, EncodeAndDecodeInt32AsVarint32") {
    std::vector<uint32_t> values = {200, 65000, 16700000, 4294000000};
    std::vector<size_t> encoded_sizes = {2, 3, 4, 5};
    for (size_t i = 0; i < values.size(); ++i) {
        std::string buf;
        PutVarint32(&buf, values[i]);
        CHECK_EQ(buf.size(), encoded_sizes[i]);
        uint32_t result = 0;
        rocksdb::Slice s(buf);
        GetVarint32(&s, &result);
        REQUIRE_EQ(result, values[i]);
    }
}
