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

#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>

#include <memory>
#include <string>
#include <vector>

#include "titandb/storage/redis_metadata.h"
#include "titandb/storage/storage.h"

namespace titandb {

    class MetadataFilter : public rocksdb::CompactionFilter {
    public:
        explicit MetadataFilter(Storage *storage) : stor_(storage) {}

        const char *Name() const override { return "MetadataFilter"; }

        bool
        Filter(int level, const Slice &key, const Slice &value, std::string *new_value, bool *modified) const override;

    private:
        Storage *stor_;
    };

    class MetadataFilterFactory : public rocksdb::CompactionFilterFactory {
    public:
        explicit MetadataFilterFactory(Storage *storage) : stor_(storage) {}

        const char *Name() const override { return "MetadataFilterFactory"; }

        std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
                const rocksdb::CompactionFilter::Context &context) override {
            return std::unique_ptr<rocksdb::CompactionFilter>(new MetadataFilter(stor_));
        }

    private:
        Storage *stor_ = nullptr;
    };

    class SubKeyFilter : public rocksdb::CompactionFilter {
    public:
        explicit SubKeyFilter(Storage *storage) : stor_(storage) {}

        const char *Name() const override { return "SubkeyFilter"; }

        turbo::Status GetMetadata(const InternalKey &ikey, Metadata *metadata) const;

        static bool IsMetadataExpired(const InternalKey &ikey, const Metadata &metadata);

        rocksdb::CompactionFilter::Decision FilterBlobByKey(int level, const Slice &key, std::string *new_value,
                                                            std::string *skip_until) const override;

        bool
        Filter(int level, const Slice &key, const Slice &value, std::string *new_value, bool *modified) const override;

    protected:
        mutable std::string cached_key_;
        mutable std::string cached_metadata_;
        Storage *stor_;
    };

    class SubKeyFilterFactory : public rocksdb::CompactionFilterFactory {
    public:
        explicit SubKeyFilterFactory(Storage *storage) : stor_(storage) {}

        const char *Name() const override { return "SubKeyFilterFactory"; }

        std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
                const rocksdb::CompactionFilter::Context &context) override {
            return std::unique_ptr<rocksdb::CompactionFilter>(new SubKeyFilter(stor_));
        }

    private:
        Storage *stor_ = nullptr;
    };

}  // namespace titandb
