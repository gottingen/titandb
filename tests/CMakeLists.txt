#
# Copyright 2023 The titan-search Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#[[carbin_cc_test(
        NAME custom_comparator_test
        SOURCES custom_comparator_test.cc
        DEPS tedis::tedis ${CARBIN_DEPS_LINK} ${GTEST_LIB} ${GTEST_MAIN_LIB}
        COPTS ${USER_CXX_FLAGS}
)]]
carbin_cc_test(
        NAME encoding_test
        SOURCES encoding_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME string_test
        SOURCES types/string_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME hash_test
        SOURCES types/hash_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME bitmap_test
        SOURCES types/bitmap_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME list_test
        SOURCES types/list_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME set_test
        SOURCES types/set_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME sortedint_test
        SOURCES types/sortedint_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

carbin_cc_test(
        NAME compact_test
        SOURCES compact_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)

#[[carbin_cc_test(
        NAME disk_test
        SOURCES disk_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK} ${GTEST_LIB} ${GTEST_MAIN_LIB}
        COPTS ${USER_CXX_FLAGS}
)]]

carbin_cc_test(
        NAME metadata_test
        SOURCES metadata_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)
#[[
carbin_cc_test(
        NAME observer_or_unique_test
        SOURCES observer_or_unique_test.cc
        DEPS titandb::titandb ${CARBIN_DEPS_LINK}
        COPTS ${USER_CXX_FLAGS}
)
]]