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
#include <string>
#include <vector>

#include "titandb/types/geohash.h"
#include "titandb/storage/redis_db.h"
#include "titandb/storage/redis_metadata.h"
#include "titandb/types/redis_zset.h"
#include "turbo/container/flat_hash_map.h"

namespace titandb {

    enum DistanceUnit {
        kDistanceMeter,
        kDistanceKilometers,
        kDistanceMiles,
        kDistanceFeet,
    };

    enum DistanceSort {
        kSortNone,
        kSortASC,
        kSortDESC,
    };

    // Structures represent points and array of points on the earth.
    struct GeoPoint {
        double longitude;
        double latitude;
        std::string member;
        double dist;
        double score;
    };


    class RedisGeo : public RedisZSet {
    public:
        explicit RedisGeo(Storage *storage, const std::string &ns) : RedisZSet(storage, ns) {}

        rocksdb::Status Add(const std::string_view &user_key, const std::vector<GeoPoint> &geo_points, int *ret);

        rocksdb::Status Dist(const std::string_view &user_key, const std::string_view &member_1, const std::string_view &member_2, double *dist);

        rocksdb::Status
        Hash(const std::string_view &user_key, const std::vector<std::string_view> &members, std::vector<std::string> *geo_hashes);

        rocksdb::Status Pos(const std::string_view &user_key, const std::vector<std::string_view> &members,
                            turbo::flat_hash_map<std::string, GeoPoint> *geo_points);

        rocksdb::Status
        Radius(const std::string_view &user_key, double longitude, double latitude, double radius_meters, int count,
               DistanceSort sort, const std::string &store_key, bool store_distance, double unit_conversion,
               std::vector<GeoPoint> *geo_points);

        rocksdb::Status RadiusByMember(const std::string_view &user_key, const std::string_view &member, double radius_meters, int count,
                                       DistanceSort sort, const std::string &store_key, bool store_distance,
                                       double unit_conversion, std::vector<GeoPoint> *geo_points);

        rocksdb::Status Get(const std::string_view &user_key, const std::string_view &member, GeoPoint *geo_point);

        rocksdb::Status MGet(const std::string_view &user_key, const std::vector<std::string_view> &members,
                             turbo::flat_hash_map<std::string, GeoPoint> *geo_points);

        static std::string EncodeGeoHash(double longitude, double latitude);

    private:
        static int decodeGeoHash(double bits, double *xy);

        int membersOfAllNeighbors(const std::string_view &user_key, GeoHashRadius n, double lon, double lat, double radius,
                                  std::vector<GeoPoint> *geo_points);

        int membersOfGeoHashBox(const std::string_view &user_key, GeoHashBits hash, std::vector<GeoPoint> *geo_points, double lon,
                                double lat, double radius);

        static void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max);

        int getPointsInRange(const std::string_view &user_key, double min, double max, double lon, double lat, double radius,
                             std::vector<GeoPoint> *geo_points);

        static bool appendIfWithinRadius(std::vector<GeoPoint> *geo_points, double lon, double lat, double radius,
                                         double score, const std::string &member);

        static bool sortGeoPointASC(const GeoPoint &gp1, const GeoPoint &gp2);

        static bool sortGeoPointDESC(const GeoPoint &gp1, const GeoPoint &gp2);
    };

}  // namespace titandb
