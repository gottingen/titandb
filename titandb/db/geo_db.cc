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

    turbo::ResultStatus<int> TitanDB::GeoAdd(const std::string_view &key, const std::vector<GeoPoint> &poi) {
        int ret = 0;
        auto s = _geo_db->Add(key, poi, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<double> TitanDB::GeoDist(const std::string_view &user_key, const std::string_view &member_1,
                                                 const std::string_view &member_2) {
        double ret = 0;
        auto s = _geo_db->Dist(user_key, member_1, member_2, &ret);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return ret;
    }

    turbo::ResultStatus<std::vector<std::string>>
    TitanDB::GeoHash(const std::string_view &user_key, const std::vector<std::string_view> &members) {
        std::vector<std::string> results;
        auto s = _geo_db->Hash(user_key, members, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<turbo::flat_hash_map<std::string, GeoPoint>>
    TitanDB::GeoPos(const std::string_view &user_key, const std::vector<std::string_view> &members) {
        turbo::flat_hash_map<std::string, GeoPoint> results;
        auto s = _geo_db->Pos(user_key, members, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<std::vector<GeoPoint>>
    TitanDB::GeoRadius(const std::string_view &user_key, double longitude, double latitude, double radius_meters,
                       int count,
                       DistanceSort sort, const std::string &store_key, bool store_distance, double unit_conversion) {
        std::vector<GeoPoint> results;
        auto s = _geo_db->Radius(user_key, longitude, latitude, radius_meters, count, sort, store_key, store_distance,
                                 unit_conversion, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }

    turbo::ResultStatus<std::vector<GeoPoint>> TitanDB::GeoRadiusByMember(const std::string_view &user_key, const std::string_view &member, double radius_meters, int count,
                                                                DistanceSort sort, const std::string &store_key, bool store_distance, double unit_conversion) {
        std::vector<GeoPoint> results;
        auto s = _geo_db->RadiusByMember(user_key, member, radius_meters, count, sort, store_key, store_distance, unit_conversion, &results);
        if (!s.ok() && !s.IsNotFound()) {
            return turbo::UnavailableError("");
        }
        if (s.IsNotFound()) {
            return turbo::NotFoundError("");
        }
        return results;
    }
}  // namespace titandb
