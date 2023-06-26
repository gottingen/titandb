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

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include "doctest/doctest.h"
#include <math.h>

#include <memory>

#include "../test_base.h"
#include "titandb/types/redis_geo.h"

using namespace titandb;

class RedisGeoTest : public TestBase {
protected:
    RedisGeoTest() {
        geo_ = std::make_unique<titandb::RedisGeo>(storage_, "geo_ns");
        key_ = "test_geo_key";
        fields_ = {"geo_test_key-1", "geo_test_key-2", "geo_test_key-3", "geo_test_key-4",
                   "geo_test_key-5", "geo_test_key-6", "geo_test_key-7"};
        longitudes_ = {-180, -1.23402, -1.23402, 0, 1.23402, 1.23402, 179.12345};
        latitudes_ = {-85.05112878, -1.23402, -1.23402, 0, 1.23402, 1.23402, 85.0511};
        geo_hashes_ = {"00bh0hbj200", "7zz0gzm7m10", "7zz0gzm7m10", "s0000000000",
                       "s00zh0dsdy0", "s00zh0dsdy0", "zzp7u51dwf0"};
    }

    ~RedisGeoTest() override = default;


    std::vector<double> longitudes_;
    std::vector<double> latitudes_;
    std::vector<std::string> geo_hashes_;
    std::unique_ptr<titandb::RedisGeo> geo_;
};

TEST_CASE_FIXTURE(RedisGeoTest, "Add") {
    int ret = 0;
    std::vector<GeoPoint> geo_points;
    for (size_t i = 0; i < fields_.size(); i++) {
        geo_points.emplace_back(
                GeoPoint{longitudes_[i], latitudes_[i], std::string{fields_[i].data(), fields_[i].size()}});
    }
    geo_->Add(key_, geo_points, &ret);
    CHECK_EQ(static_cast<int>(fields_.size()), ret);
    std::vector<std::string> geo_hashes;
    geo_->Hash(key_, fields_, &geo_hashes);
    for (size_t i = 0; i < fields_.size(); i++) {
        CHECK_EQ(geo_hashes[i], geo_hashes_[i]);
    }
    geo_->Add(key_, geo_points, &ret);
    CHECK_EQ(ret, 0);
    geo_->Del(key_);
}

TEST_CASE_FIXTURE(RedisGeoTest, "Dist") {
    int ret = 0;
    std::vector<GeoPoint> geo_points;
    for (size_t i = 0; i < fields_.size(); i++) {
        geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], std::string(fields_[i])});
    }
    geo_->Add(key_, geo_points, &ret);
    CHECK_EQ(fields_.size(), ret);
    double dist = 0.0;
    geo_->Dist(key_, fields_[2], fields_[3], &dist);
    CHECK_EQ(ceilf(dist), 194102);
    geo_->Del(key_);
}

TEST_CASE_FIXTURE(RedisGeoTest, "RedisHash") {
    int ret = 0;
    std::vector<GeoPoint> geo_points;
    for (size_t i = 0; i < fields_.size(); i++) {
        geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], std::string(fields_[i])});
    }
    geo_->Add(key_, geo_points, &ret);
    CHECK_EQ(static_cast<int>(fields_.size()), ret);
    std::vector<std::string> geo_hashes;
    geo_->Hash(key_, fields_, &geo_hashes);
    for (size_t i = 0; i < fields_.size(); i++) {
        CHECK_EQ(geo_hashes[i], geo_hashes_[i]);
    }
    geo_->Del(key_);
}

TEST_CASE_FIXTURE(RedisGeoTest, "Pos") {
    int ret = 0;
    std::vector<GeoPoint> geo_points;
    for (size_t i = 0; i < fields_.size(); i++) {
        geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], std::string(fields_[i])});
    }
    geo_->Add(key_, geo_points, &ret);
    CHECK_EQ(static_cast<int>(fields_.size()), ret);
    turbo::flat_hash_map<std::string, GeoPoint> gps;
    geo_->Pos(key_, fields_, &gps);
    for (size_t i = 0; i < fields_.size(); i++) {
        CHECK_EQ(gps[std::string(fields_[i])].member, std::string(fields_[i]));
        CHECK_EQ(geo_->EncodeGeoHash(gps[std::string(fields_[i])].longitude, gps[std::string(fields_[i])].latitude),
                 geo_hashes_[i]);
    }
    geo_->Del(key_);
}

TEST_CASE_FIXTURE(RedisGeoTest, "Radius") {
    int ret = 0;
    std::vector<GeoPoint> geo_points;
    for (size_t i = 0; i < fields_.size(); i++) {
        geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], std::string(fields_[i])});
    }
    geo_->Add(key_, geo_points, &ret);
    CHECK_EQ(static_cast<int>(fields_.size()), ret);
    std::vector<GeoPoint> gps;
    geo_->Radius(key_, longitudes_[0], latitudes_[0], 100000000, 100, kSortASC, std::string(), false, 1, &gps);
    CHECK_EQ(gps.size(), fields_.size());
    for (size_t i = 0; i < gps.size(); i++) {
        CHECK_EQ(gps[i].member, std::string(fields_[i]));
        CHECK_EQ(geo_->EncodeGeoHash(gps[i].longitude, gps[i].latitude), geo_hashes_[i]);
    }
    geo_->Del(key_);
}

TEST_CASE_FIXTURE(RedisGeoTest, "RadiusByMember") {
    int ret = 0;
    std::vector<GeoPoint> geo_points;
    for (size_t i = 0; i < fields_.size(); i++) {
        geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], std::string(fields_[i])});
    }
    geo_->Add(key_, geo_points, &ret);
    CHECK_EQ(static_cast<int>(fields_.size()), ret);
    std::vector<GeoPoint> gps;
    geo_->RadiusByMember(key_, fields_[0], 100000000, 100, kSortASC, std::string(), false, 1, &gps);
    CHECK_EQ(gps.size(), fields_.size());
    for (size_t i = 0; i < gps.size(); i++) {
        CHECK_EQ(gps[i].member, std::string(fields_[i]));
        CHECK_EQ(geo_->EncodeGeoHash(gps[i].longitude, gps[i].latitude), geo_hashes_[i]);
    }
    geo_->Del(key_);
}
