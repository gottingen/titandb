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

#include "titandb/types/range_spec.h"

namespace titandb {

    turbo::Status RangeLexSpec::Parse(const std::string_view &min_arg, const std::string_view &max_arg) {
        if (min_arg == "+" || max_arg == "-") {
            return turbo::InvalidArgumentError("min_arg > max_arg");
        }

        if (min_arg == "-") {
            this->min = "";
        } else {
            if (min_arg[0] == '(') {
                this->minex = true;
            } else if (min_arg[0] == '[') {
                this->minex = false;
            } else {
                return turbo::InvalidArgumentError("the min_arg is illegal");
            }
            this->min = min_arg.substr(1);
        }

        if (max_arg == "+") {
            this->max_infinite = true;
        } else {
            if (max_arg[0] == '(') {
                this->maxex = true;
            } else if (max_arg[0] == '[') {
                this->maxex = false;
            } else {
                return turbo::InvalidArgumentError("the max_arg is illegal");
            }
            this->max = max_arg.substr(1);
        }
        return turbo::OkStatus();
    }
}