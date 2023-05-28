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

#include <limits>
#include <map>
#include <string>
#include <vector>

#include "titandb/types/range_spec.h"
#include "titandb/storage/redis_db.h"
#include "titandb/storage/redis_metadata.h"
#include "turbo/container/flat_hash_map.h"

namespace titandb {


    enum AggregateMethod {
        kAggregateSum, kAggregateMin, kAggregateMax
    };

    struct KeyWeight {
        std::string key;
        double weight;
    };

    struct MemberScore {
        std::string member;
        double score;
    };

    enum ZRangeType {
        kZRangeAuto,
        kZRangeRank,
        kZRangeScore,
        kZRangeLex,
    };

    enum ZRangeDirection {
        kZRangeDirectionAuto,
        kZRangeDirectionForward,
        kZRangeDirectionReverse,
    };

    enum ZSetFlags {
        kZSetIncr = 1,
        kZSetNX = 1 << 1,
        kZSetXX = 1 << 2,
        kZSetGT = 1 << 3,
        kZSetLT = 1 << 4,
        kZSetCH = 1 << 5,
    };

    class ZAddFlags {
    public:
        explicit ZAddFlags(uint8_t flags = 0) : flags_(flags) {}

        [[nodiscard]] bool HasNX() const { return (flags_ & kZSetNX) != 0; }

        [[nodiscard]] bool HasXX() const { return (flags_ & kZSetXX) != 0; }

        [[nodiscard]] bool HasLT() const { return (flags_ & kZSetLT) != 0; }

        [[nodiscard]] bool HasGT() const { return (flags_ & kZSetGT) != 0; }

        [[nodiscard]] bool HasCH() const { return (flags_ & kZSetCH) != 0; }

        [[nodiscard]] bool HasIncr() const { return (flags_ & kZSetIncr) != 0; }

        [[nodiscard]] bool HasAnyFlags() const { return flags_ != 0; }

        void SetFlag(ZSetFlags set_flags) { flags_ |= set_flags; }

        static ZAddFlags Incr() { return ZAddFlags{kZSetIncr}; }

        static ZAddFlags Default() { return ZAddFlags{0}; }

    private:
        uint8_t flags_ = 0;
    };


    class RedisZSet : public SubKeyScanner {
    public:
        explicit RedisZSet(Storage *storage, const std::string &ns)
                : SubKeyScanner(storage, ns), score_cf_handle_(storage->GetCFHandle("zset_score")) {}

        using Members = std::vector<std::string>;
        using MemberScores = std::vector<MemberScore>;

        rocksdb::Status Add(const std::string_view &user_key, ZAddFlags flags, MemberScores *mscores, int *ret);

        rocksdb::Status Card(const std::string_view &user_key, int *ret);

        rocksdb::Status
        IncrBy(const std::string_view &user_key, const std::string_view &member, double increment, double *score);

        rocksdb::Status Rank(const std::string_view &user_key, const std::string_view &member, bool reversed, int *ret);

        rocksdb::Status
        Remove(const std::string_view &user_key, const std::vector<std::string_view> &members, int *ret);

        rocksdb::Status Pop(const std::string_view &user_key, int count, bool min, MemberScores *mscores);

        rocksdb::Status Score(const std::string_view &user_key, const std::string_view &member, double *score);

        rocksdb::Status Scan(const std::string_view &user_key, const std::string_view &cursor, uint64_t limit,
                             const std::string_view &member_prefix, std::vector<std::string> *members,
                             std::vector<double> *scores = nullptr);

        rocksdb::Status Overwrite(const std::string_view &user_key, const MemberScores &mscores);

        rocksdb::Status InterStore(const std::string_view &dst, const std::vector<KeyWeight> &keys_weights,
                                   AggregateMethod aggregate_method, int *size);

        rocksdb::Status UnionStore(const std::string_view &dst, const std::vector<KeyWeight> &keys_weights,
                                   AggregateMethod aggregate_method, int *size);

        rocksdb::Status
        MGet(const std::string_view &user_key, const std::vector<std::string_view> &members,
             turbo::flat_hash_map<std::string, double> *scores);

        rocksdb::Status GetMetadata(const std::string_view &ns_key, ZSetMetadata *metadata);

        rocksdb::Status Count(const std::string_view &user_key, const RangeScoreSpec &spec, int *ret);

        rocksdb::Status
        RangeByRank(const std::string_view &user_key, const RangeRankSpec &spec, MemberScores *mscores, int *ret);

        rocksdb::Status
        RangeByScore(const std::string_view &user_key, const RangeScoreSpec &spec, MemberScores *mscores, int *ret);

        rocksdb::Status
        RangeByLex(const std::string_view &user_key, const RangeLexSpec &spec, Members *members, int *ret);

    private:
        rocksdb::ColumnFamilyHandle *score_cf_handle_;
    };

}  // namespace titandb
