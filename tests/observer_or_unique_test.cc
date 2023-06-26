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

#include "titandb/common/observer_or_unique.h"

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest/doctest.h"

struct Counter {  // NOLINT
    explicit Counter(int *i) : i(i) { ++*i; }

    ~Counter() { --*i; }

    int *i;
};

TEST(ObserverOrUniquePtr, Unique) {
    int v = 0;
    {
        ObserverOrUniquePtr<Counter> unique(new Counter{&v}, ObserverOrUnique::Unique);
        REQUIRE_EQ(v, 1);
    }
    REQUIRE_EQ(v, 0);
}

TEST(ObserverOrUniquePtr, Observer) {
    int v = 0;
    std::unique_ptr<Counter> c = nullptr;
    {
        ObserverOrUniquePtr<Counter> observer(new Counter{&v}, ObserverOrUnique::Observer);
        REQUIRE_EQ(v, 1);

        c.reset(observer.Get());
    }
    REQUIRE_EQ(v, 1);
    c.reset();
    REQUIRE_EQ(v, 0);
}
