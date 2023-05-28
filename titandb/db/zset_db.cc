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
#include "titandb/db.h"

namespace titandb {

    turbo::ResultStatus<int>
    TitanDB::ZAdd(const std::string_view &user_key, std::vector<MemberScore> *ms, ZAddFlags flags) {
        int ret;
        if(flags.HasIncr()) {
            return turbo::UnavailableError("using IncrBy api");
        }
        auto s = _zset_db->Add(user_key, flags, ms, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::ZCard(const std::string_view &key) {
        int ret;
        auto s = _zset_db->Card(key, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int>
    TitanDB::ZCount(const std::string_view &key, double min, double max, bool ex_min, bool ex_max) {
        int ret;
        RangeScoreSpec spec;
        spec.max = max;
        spec.min = min;
        spec.minex = ex_min;
        spec.maxex = ex_max;
        auto s = _zset_db->Count(key, spec, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<double>
    TitanDB::ZIncrBy(const std::string_view &key, const std::string_view &member, double score) {
        double ret;
        auto s = _zset_db->IncrBy(key, member, score, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int>
    TitanDB::ZInterStore(const std::string_view &dst, const std::vector<KeyWeight> &keys_weights,
                         AggregateMethod aggregate_method) {
        int ret;
        auto s = _zset_db->InterStore(dst, keys_weights, aggregate_method, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int>
    TitanDB::ZLexCount(const std::string_view &key, const std::string_view &min, const std::string_view &max) {
        RangeLexSpec spec;
        int ret;
        auto rs = spec.Parse(min, max);
        if (!rs.ok()) {
            return rs;
        }
        auto s = _zset_db->RangeByLex(key, spec, nullptr, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<MemberScore>> TitanDB::ZPopMax(const std::string_view &key, int count) {
        std::vector<MemberScore> ret;
        auto s = _zset_db->Pop(key, count, false, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<MemberScore>> TitanDB::ZPopMin(const std::string_view &key, int count) {
        std::vector<MemberScore> ret;
        auto s = _zset_db->Pop(key, count, true, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<MemberScore>> TitanDB::ZRange(const std::string_view &key, int start, int stop) {
        RangeRankSpec spec;
        spec.reversed = false;
        spec.start = start;
        spec.stop = stop;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->RangeByRank(key, spec, &member_scores, nullptr);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return member_scores;
    }

    turbo::ResultStatus<std::vector<MemberScore>> TitanDB::ZRevRange(const std::string_view &key, int start, int stop) {
        RangeRankSpec spec;
        spec.reversed = true;
        spec.start = start;
        spec.stop = stop;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->RangeByRank(key, spec, &member_scores, nullptr);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return member_scores;
    }

    turbo::ResultStatus<std::vector<std::string>>
    TitanDB::ZRangeByLex(const std::string_view &key, const std::string_view &min, const std::string_view &max,
                         int offset, int count, bool rev) {
        RangeLexSpec spec;
        int ret;
        auto rs = spec.Parse(min, max);
        if (!rs.ok()) {
            return rs;
        }
        spec.count = count;
        spec.offset = offset;
        spec.reversed = rev;
        std::vector<std::string> member_scores;
        auto s = _zset_db->RangeByLex(key, spec, &member_scores, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return member_scores;
    }

    turbo::ResultStatus<std::vector<MemberScore>>
    TitanDB::ZRangeByScore(const std::string_view &key, double min, double max, int offset, int count, bool ex_min,
                           bool ex_max) {
        RangeScoreSpec spec;
        spec.min = min;
        spec.max = max;
        spec.minex = ex_min;
        spec.maxex = ex_max;
        spec.reversed = false;
        spec.offset = offset;
        spec.count = count;
        int ret;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->RangeByScore(key, spec, &member_scores, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return member_scores;
    }

    turbo::ResultStatus<int> TitanDB::ZRank(const std::string_view &key, const std::string_view &member) {
        int ret;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->Rank(key, member, false, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::ZRem(const std::string_view &key, std::vector<std::string_view> &members) {
        int ret;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->Remove(key, members, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int> TitanDB::ZRemRangeByRank(const std::string_view &key, int start, int stop) {
        RangeRankSpec spec;
        spec.reversed = true;
        spec.start = start;
        spec.stop = stop;
        spec.with_deletion = true;
        int ret;
        auto s = _zset_db->RangeByRank(key, spec, nullptr, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int>
    TitanDB::ZRemRangeByScore(const std::string_view &key, double min, double max, bool ex_min, bool ex_max) {
        RangeScoreSpec spec;
        spec.min = min;
        spec.max = max;
        spec.minex = ex_min;
        spec.maxex = ex_max;
        spec.offset = 0;
        spec.count = std::numeric_limits<int32_t>::max();
        spec.with_deletion = true;
        int ret;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->RangeByScore(key, spec, nullptr, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<int>
    TitanDB::ZRemRangeByLex(const std::string_view &key, const std::string_view &min, const std::string_view &max) {
        RangeLexSpec spec;
        int ret;
        auto rs = spec.Parse(min, max);
        if (!rs.ok()) {
            return rs;
        }
        spec.reversed = false;
        auto s = _zset_db->RangeByLex(key, spec, nullptr, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }
    turbo::ResultStatus<std::vector<MemberScore>> TitanDB::ZRevRangeByScore(const std::string_view &key, double min, double max, int offset,
                                                                   int count, bool ex_min,
                                                                   bool ex_max) {
        RangeScoreSpec spec;
        spec.min = min;
        spec.max = max;
        spec.minex = ex_min;
        spec.maxex = ex_max;
        spec.reversed = true;
        spec.offset = offset;
        spec.count = count;
        int ret;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->RangeByScore(key, spec, &member_scores, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return member_scores;

    }

    turbo::ResultStatus<int> TitanDB::ZRevRank(const std::string_view &key, const std::string_view &member) {
        int ret;
        std::vector<MemberScore> member_scores;
        auto s = _zset_db->Rank(key, member, true, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<double> TitanDB::ZScore(const std::string_view &key, const std::string_view &member) {
        double ret;
        auto s = _zset_db->Score(key, member, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<double>> TitanDB::ZMScore(const std::string_view &key, const std::vector<std::string_view> &members, double not_found_score) {
        turbo::flat_hash_map<std::string, double> mscores;
        auto s = _zset_db->MGet(key, members, &mscores);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        std::vector<double> ret(members.size(), not_found_score);
        for(size_t i = 0; i < members.size(); i++) {
            auto it = mscores.find(members[i]);
            if( it != mscores.end()) {
                ret[i] = it->second;
            }
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<MemberScore>> TitanDB::ZScan(const std::string_view &key, int start, const std::string_view &cursor, const std::string_view &prefix, int limit) {
        std::vector<std::string> members;
        std::vector<double> scores;
        auto s = _zset_db->Scan(key, cursor, limit, prefix, &members, &scores);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        std::vector<MemberScore> msocres;
        msocres.reserve(members.size());
        for(size_t i = 0; i < members.size(); i++) {
            msocres.emplace_back(MemberScore{std::move(members[i]), scores[i]});
        }
        return msocres;
    }

    turbo::ResultStatus<int> TitanDB::ZUnionStore(const std::string_view &key, const std::vector<KeyWeight> & kws, AggregateMethod am) {
        int ret;
        auto s = _zset_db->UnionStore(key, kws, am, &ret);
        if (!s.ok()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }
}  // namespace titandb