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

#include "titandb/common/string_util.h"

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest/doctest.h"

#include <map>


TEST(StringUtil, Trim) {
    std::map<std::string, std::string> cases{
            {"abc",         "abc"},
            {"   abc    ",  "abc"},
            {"\t\tabc\t\t", "abc"},
            {"\t\tabc\n\n", "abc"},
            {"\n\nabc\n\n", "abc"},
            {" a b",        "a b"},
            {"a \tb\t \n",  "a \tb"},
    };
    for (auto iter = cases.begin(); iter != cases.end(); iter++) {
        std::string input = iter->first;
        std::string output = titandb::Trim(input, " \t\n");
        REQUIRE_EQ(output, iter->second);
    }
}


TEST(StringUtil, TokenizeRedisProtocol) {
    std::vector<std::string> expected = {"this", "is", "a", "test"};
    auto array = titandb::TokenizeRedisProtocol("*4\r\n$4\r\nthis\r\n$2\r\nis\r\n$1\r\na\r\n$4\r\ntest\r\n");
    REQUIRE_EQ(expected, array);
}

TEST(StringUtil, HasPrefix) {
    REQUIRE(titandb::HasPrefix("has_prefix_is_true", "has_prefix"));
    ASSERT_FALSE(titandb::HasPrefix("has_prefix_is_false", "_has_prefix"));
    REQUIRE(titandb::HasPrefix("has_prefix", "has_prefix"));
    ASSERT_FALSE(titandb::HasPrefix("has", "has_prefix"));
}
