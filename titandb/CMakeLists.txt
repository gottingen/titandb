#
# Copyright 2023 The Carbin Authors.
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

file(GLOB ST_SRC  "storage/*.cc")
file(GLOB COM_SRC  "common/*.cc")
file(GLOB TY_SRC  "types/*.cc")
file(GLOB SS_SRC  "stats/*.cc")
file(GLOB CONF_SRC  "config/*.cc")
file(GLOB DB_SRC  "db/*.cc")

set(TEDIS_SRC
        db.cc
        ${ST_SRC}
        ${COM_SRC}
        ${TY_SRC}
        ${SS_SRC}
        ${CONF_SRC}
        ${DB_SRC}
        )

carbin_cc_library(
        NAMESPACE titandb
        NAME titandb
        SOURCES
        ${TEDIS_SRC}
        COPTS
        ${USER_CXX_FLAGS}
        DEPS
        ${CARBIN_DEPS_LINK}
        PUBLIC
)
