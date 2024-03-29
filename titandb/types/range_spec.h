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

#pragma once

#include <string>
#include <limits>
#include "turbo/base/status.h"

namespace titandb {
    struct RangeLexSpec {
        std::string min, max;
        bool minex = false, maxex = false; /* are min or max exclusive */
        bool max_infinite = false;         /* are max infinite */
        int64_t offset = -1, count = -1;
        bool with_deletion = false, reversed = false;

        explicit RangeLexSpec() = default;

        turbo::Status Parse(const std::string_view &min_arg, const std::string_view &max_arg);
    };

    struct RangeRankSpec {
        int start = 0, stop = -1;
        bool with_deletion = false, reversed = false;

        explicit RangeRankSpec() = default;
    };


    const double kMinScore = (std::numeric_limits<float>::is_iec559 ? -std::numeric_limits<double>::infinity()
                                                                    : std::numeric_limits<double>::lowest());
    const double kMaxScore = (std::numeric_limits<float>::is_iec559 ? std::numeric_limits<double>::infinity()
                                                                    : std::numeric_limits<double>::max());

    struct RangeScoreSpec {
        double min = kMinScore, max = kMaxScore;
        bool minex = false, maxex = false; /* are min or max exclusive */
        int64_t offset = -1, count = -1;
        bool with_deletion = false, reversed = false;

        explicit RangeScoreSpec() = default;
    };

}  // namespace titandb
