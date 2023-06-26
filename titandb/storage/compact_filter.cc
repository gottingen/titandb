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

#include "titandb/storage/compact_filter.h"

#include "turbo/log/logging.h"

#include <string>
#include <utility>

#include "turbo/times/clock.h"
#include "titandb/types/redis_bitmap.h"

namespace titandb {

    using rocksdb::Slice;

    bool MetadataFilter::Filter(int level, const Slice &key, const Slice &value, std::string *new_value,
                                bool *modified) const {
        std::string ns, user_key, bytes = value.ToString();
        Metadata metadata(kRedisNone, false);
        rocksdb::Status s = metadata.Decode(bytes);
        ExtractNamespaceKey(key, &ns, &user_key, stor_->IsSlotIdEncoded());
        if (!s.ok()) {
            TLOG_WARN("[compact_filter/metadata] Failed to decode, namespace: {}, key: {}, err: {}", ns, user_key,
                      s.ToString());
            return false;
        }
        TLOG_INFO("[compact_filter/metadata] namespace: {}, key {}, result: {}", ns, user_key,
                  (metadata.Expired() ? "deleted" : "reserved"));
        return metadata.Expired();
    }

    turbo::Status SubKeyFilter::GetMetadata(const InternalKey &ikey, Metadata *metadata) const {
        std::string metadata_key;

        auto db = stor_->GetDB();
        const auto cf_handles = stor_->GetCFHandles();
        // storage close the would delete the column family handler and DB
        if (!db || cf_handles->size() < 2) return turbo::UnavailableError("storage is closed");
        ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), &metadata_key, stor_->IsSlotIdEncoded());

        if (cached_key_.empty() || metadata_key != cached_key_) {
            std::string bytes;
            rocksdb::Status s = db->Get(rocksdb::ReadOptions(), (*cf_handles)[1], metadata_key, &bytes);
            cached_key_ = std::move(metadata_key);
            if (s.ok()) {
                cached_metadata_ = std::move(bytes);
            } else if (s.IsNotFound()) {
                // metadata was deleted(perhaps compaction or manual)
                // clear the metadata
                cached_metadata_.clear();
                return turbo::NotFoundError("metadata is not found");
            } else {
                cached_key_.clear();
                cached_metadata_.clear();
                return turbo::UnavailableError("fetch error: " + s.ToString());
            }
        }
        // the metadata was not found
        if (cached_metadata_.empty()) return turbo::NotFoundError("metadata is not found");
        // the metadata is cached
        rocksdb::Status s = metadata->Decode(cached_metadata_);
        if (!s.ok()) {
            cached_key_.clear();
            return turbo::UnavailableError("decode error: " + s.ToString());
        }
        return turbo::OkStatus();
    }

    bool SubKeyFilter::IsMetadataExpired(const InternalKey &ikey, const Metadata &metadata) {
        // lazy delete to avoid race condition between command Expire and subkey Compaction
        // Related issue:https://github.com/apache/incubator-kvrocks/issues/1298
        //
        // `Util::turbo::ToUnixMillis(turbo::Now()) - 300000` means extending 5 minutes for expired items,
        // to prevent them from being recycled once they reach the expiration time.
        uint64_t lazy_expired_ts = turbo::ToUnixMillis(turbo::Now()) - 300000;
        return metadata.Type() == kRedisString  // metadata key was overwrite by set command
               || metadata.ExpireAt(lazy_expired_ts) || ikey.GetVersion() != metadata.version;
    }

    rocksdb::CompactionFilter::Decision
    SubKeyFilter::FilterBlobByKey(int level, const Slice &key, std::string *new_value,
                                  std::string *skip_until) const {
        InternalKey ikey(key, stor_->IsSlotIdEncoded());
        Metadata metadata(kRedisNone, false);
        auto s = GetMetadata(ikey, &metadata);
        if (turbo::IsNotFound(s)) {
            return rocksdb::CompactionFilter::Decision::kRemove;
        }
        if (!s.ok()) {
            TLOG_ERROR("[compact_filter/subkey] Failed to get metadata, namespace: {}, key: {}, err: {}",
                       ikey.GetNamespace().ToString(), ikey.GetKey().ToString(), s.message());
            return rocksdb::CompactionFilter::Decision::kKeep;
        }
        // bitmap will be checked in Filter
        if (metadata.Type() == kRedisBitmap) {
            return rocksdb::CompactionFilter::Decision::kUndetermined;
        }

        bool result = IsMetadataExpired(ikey, metadata);
        return result ? rocksdb::CompactionFilter::Decision::kRemove : rocksdb::CompactionFilter::Decision::kKeep;
    }

    bool SubKeyFilter::Filter(int level, const Slice &key, const Slice &value, std::string *new_value,
                              bool *modified) const {
        InternalKey ikey(key, stor_->IsSlotIdEncoded());
        Metadata metadata(kRedisNone, false);
        auto s = GetMetadata(ikey, &metadata);
        if (turbo::IsNotFound(s)) {
            return true;
        }
        if (!s.ok()) {
            TLOG_ERROR("[compact_filter/subkey] Failed to get metadata, namespace: {}, key: {}, err: {}",
                       ikey.GetNamespace().ToString(),
                       ikey.GetKey().ToString(), s.message());
            return false;
        }

        return IsMetadataExpired(ikey, metadata) ||
               (metadata.Type() == kRedisBitmap && RedisBitmap::IsEmptySegment(value));
    }

}  // namespace titandb
