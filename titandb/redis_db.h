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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "titandb/types/range_spec.h"
#include "titandb/storage/redis_metadata.h"
#include "titandb/storage/storage.h"

namespace titandb {

    struct StringPair {
        std::string_view key;
        std::string_view value;
    };

    struct SortedintRangeSpec {
        uint64_t min = std::numeric_limits<uint64_t>::lowest(), max = std::numeric_limits<uint64_t>::max();
        bool minex = false, maxex = false; /* are min or max exclusive */
        int offset = -1, count = -1;
        bool reversed = false;

        SortedintRangeSpec() = default;
    };

    struct FieldValue {
        std::string field;
        std::string value;

        FieldValue(std::string f, std::string v) : field(std::move(f)), value(std::move(v)) {}
    };

    struct FieldValueArray {
        std::vector<std::string> fields;
        std::vector<std::string> values;

        FieldValueArray(std::vector<std::string> fs, std::vector<std::string> vs) : fields(std::move(fs)),
                                                                                    values(std::move(vs)) {}
    };

    enum class HashFetchType {
        kAll = 0, kOnlyKey = 1, kOnlyValue = 2
    };

    enum BitOpFlags {
        kBitOpAnd,
        kBitOpOr,
        kBitOpXor,
        kBitOpNot,
    };

    class RedisDB {
    public:
        static constexpr uint64_t RANDOM_KEY_SCAN_LIMIT = 60;

        explicit RedisDB(titandb::Storage *storage, std::string ns = "");

        void Refresh();

        rocksdb::Status GetMetadata(RedisType type, const Slice &ns_key, Metadata *metadata);

        rocksdb::Status GetRawMetadata(const Slice &ns_key, std::string *bytes);

        rocksdb::Status GetRawMetadataByUserKey(const Slice &user_key, std::string *bytes);

        rocksdb::Status Expire(const Slice &user_key, uint64_t timestamp);

        rocksdb::Status Del(const Slice &user_key);

        rocksdb::Status Exists(const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status TTL(const Slice &user_key, int64_t *ttl);

        rocksdb::Status Type(const Slice &user_key, RedisType *type);

        rocksdb::Status Dump(const Slice &user_key, std::vector<std::string> *infos);

        rocksdb::Status FlushDB();

        rocksdb::Status FlushAll();

        void GetKeyNumStats(const std::string &prefix, KeyNumStats *stats);

        void Keys(const std::string &prefix, std::vector<std::string> *keys = nullptr, KeyNumStats *stats = nullptr);

        rocksdb::Status Scan(const Slice &cursor, uint64_t limit, const Slice &prefix,
                             std::vector<std::string> *keys, std::string *end_cursor = nullptr);

        rocksdb::Status RandomKey(const std::string &cursor, std::string *key);

        void AppendNamespacePrefix(const Slice &user_key, std::string *output);

        rocksdb::Status
        FindKeyRangeWithPrefix(const std::string &prefix, const std::string &prefix_end, std::string *begin,
                               std::string *end, rocksdb::ColumnFamilyHandle *cf_handle = nullptr);

        // string
        rocksdb::Status Append(const std::string_view &user_key, const std::string_view &value, int *ret);

        rocksdb::Status Get(const std::string_view &user_key, std::string *value);

        rocksdb::Status GetEx(const std::string_view &user_key, std::string *value, uint64_t ttl);

        rocksdb::Status GetSet(const std::string_view &user_key, const std::string_view &new_value, std::string *old_value);

        rocksdb::Status GetDel(const std::string_view &user_key, std::string *value);

        rocksdb::Status Set(const std::string_view &user_key, const std::string_view &value);

        rocksdb::Status SetEX(const std::string_view &user_key, const std::string_view &value, uint64_t ttl);

        rocksdb::Status SetNX(const std::string_view &user_key, const std::string_view &value, uint64_t ttl, int *ret);

        rocksdb::Status SetXX(const std::string_view &user_key, const std::string_view &value, uint64_t ttl, int *ret);

        rocksdb::Status SetRange(const std::string_view &user_key, size_t offset, const std::string_view &value, int *ret);

        rocksdb::Status IncrBy(const std::string_view &user_key, int64_t increment, int64_t *ret);

        rocksdb::Status IncrByFloat(const std::string_view &user_key, double increment, double *ret);

        std::vector<rocksdb::Status> MGet(const std::vector<std::string_view> &keys, std::vector<std::string> *values);

        rocksdb::Status MSet(const std::vector<StringPair> &pairs, uint64_t ttl = 0);

        rocksdb::Status MSetNX(const std::vector<StringPair> &pairs, uint64_t ttl, int *ret);

        rocksdb::Status CAS(const std::string_view &user_key, const std::string_view &old_value, const std::string_view &new_value,
                            uint64_t ttl, int *ret);

        rocksdb::Status CAD(const std::string_view &user_key, const std::string_view &value, int *ret);

        // sorted int
        rocksdb::Status SICard(const Slice &user_key, int *ret);

        rocksdb::Status SIMExist(const Slice &user_key, const std::vector<uint64_t> &ids, std::vector<int> *exists);

        rocksdb::Status SIAdd(const Slice &user_key, const std::vector<uint64_t> &ids, int *ret);

        rocksdb::Status SIRemove(const Slice &user_key, const std::vector<uint64_t> &ids, int *ret);

        rocksdb::Status SIRange(const Slice &user_key, uint64_t cursor_id, uint64_t page, uint64_t limit, bool reversed,
                              std::vector<uint64_t> *ids);

        rocksdb::Status
        SIRangeByValue(const Slice &user_key, SortedintRangeSpec spec, std::vector<uint64_t> *ids, int *size);

        static turbo::Status SIParseRangeSpec(const std::string &min, const std::string &max, SortedintRangeSpec *spec);

        // set
        rocksdb::Status Card(const std::string_view &user_key, int *ret);

        rocksdb::Status IsMember(const std::string_view &user_key, const std::string_view &member, int *ret);

        rocksdb::Status MIsMember(const std::string_view &user_key, const std::vector<std::string_view> &members, std::vector<int> *exists);

        rocksdb::Status Add(const std::string_view &user_key, const std::vector<std::string_view> &members, int *ret);

        rocksdb::Status Remove(const std::string_view &user_key, const std::vector<std::string_view> &members, int *ret);

        rocksdb::Status Members(const std::string_view &user_key, std::vector<std::string> *members);

        rocksdb::Status Move(const std::string_view &src, const std::string_view &dst, const std::string_view &member, int *ret);

        rocksdb::Status Take(const std::string_view &user_key, std::vector<std::string> *members, int count, bool pop);

        rocksdb::Status Diff(const std::vector<std::string_view> &keys, std::vector<std::string> *members);

        rocksdb::Status Union(const std::vector<std::string_view> &keys, std::vector<std::string> *members);

        rocksdb::Status Inter(const std::vector<std::string_view> &keys, std::vector<std::string> *members);

        rocksdb::Status Overwrite(std::string_view user_key, const std::vector<std::string> &members);

        rocksdb::Status DiffStore(const std::string_view &dst, const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status UnionStore(const std::string_view &dst, const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status InterStore(const std::string_view &dst, const std::vector<std::string_view> &keys, int *ret);

        rocksdb::Status Scan(const std::string_view &user_key, const std::string_view &cursor, uint64_t limit,
                             const std::string_view &member_prefix, std::vector<std::string> *members);

        // list
        rocksdb::Status Size(const Slice &user_key, uint32_t *ret);

        rocksdb::Status Trim(const Slice &user_key, int start, int stop);

        rocksdb::Status Set(const Slice &user_key, int index, Slice elem);

        rocksdb::Status Insert(const Slice &user_key, const Slice &pivot, const Slice &elem, bool before, int *ret);

        rocksdb::Status Pop(const Slice &user_key, bool left, std::string *elem);

        rocksdb::Status PopMulti(const Slice &user_key, bool left, uint32_t count, std::vector<std::string> *elems);

        rocksdb::Status Rem(const Slice &user_key, int count, const Slice &elem, int *ret);

        rocksdb::Status Index(const Slice &user_key, int index, std::string *elem);

        rocksdb::Status RPopLPush(const Slice &src, const Slice &dst, std::string *elem);

        rocksdb::Status LMove(const Slice &src, const Slice &dst, bool src_left, bool dst_left, std::string *elem);

        rocksdb::Status Push(const Slice &user_key, const std::vector<std::string_view> &elems, bool left, int *ret);

        rocksdb::Status PushX(const Slice &user_key, const std::vector<std::string_view> &elems, bool left, int *ret);

        rocksdb::Status Range(const Slice &user_key, int start, int stop, std::vector<std::string> *elems);

        // hash
        rocksdb::Status HashSize(const Slice &user_key, uint32_t *ret);

        rocksdb::Status Get(const Slice &user_key, const Slice &field, std::string *value);

        rocksdb::Status Set(const Slice &user_key, const Slice &field, const Slice &value, int *ret);

        rocksdb::Status Delete(const Slice &user_key, const std::vector<std::string_view> &fields, int *ret);

        rocksdb::Status IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *ret);

        rocksdb::Status IncrByFloat(const Slice &user_key, const Slice &field, double increment, double *ret);

        rocksdb::Status MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx, int *ret);

        rocksdb::Status
        RangeByLex(const Slice &user_key, const RangeLexSpec &spec, std::vector<FieldValue> *field_values);

        rocksdb::Status
        MGet(const Slice &user_key, const std::vector<std::string_view> &fields, std::vector<std::string> *values,
             std::vector<rocksdb::Status> *statuses);

        rocksdb::Status GetAll(const Slice &user_key, std::vector<FieldValue> *field_values,
                               HashFetchType type = HashFetchType::kAll);

        rocksdb::Status Scan(const Slice &user_key, const Slice &cursor, uint64_t limit,
                             const Slice &field_prefix, std::vector<std::string> *fields,
                             std::vector<std::string> *values = nullptr);

        // bit string
        static rocksdb::Status StringGetBit(const std::string &raw_value, uint32_t offset, bool *bit);

        rocksdb::Status
        StringSetBit(const Slice &ns_key, std::string *raw_value, uint32_t offset, bool new_bit, bool *old_bit);

        static rocksdb::Status StringBitCount(const std::string &raw_value, int64_t start, int64_t stop, uint32_t *cnt);

        static rocksdb::Status
        StringBitPos(const std::string &raw_value, bool bit, int64_t start, int64_t stop, bool stop_given,
               int64_t *pos);

        static size_t RawPopcount(const uint8_t *p, int64_t count);

        static int64_t RawBitpos(const uint8_t *c, int64_t count, bool bit);

        // bitmap
        rocksdb::Status GetBit(const Slice &user_key, uint32_t offset, bool *bit);

        rocksdb::Status GetString(const Slice &user_key, uint32_t max_btos_size, std::string *value);

        rocksdb::Status SetBit(const Slice &user_key, uint32_t offset, bool new_bit, bool *old_bit);

        rocksdb::Status BitCount(const Slice &user_key, int64_t start, int64_t stop, uint32_t *cnt);

        rocksdb::Status
        BitPos(const Slice &user_key, bool bit, int64_t start, int64_t stop, bool stop_given, int64_t *pos);

        rocksdb::Status BitOp(BitOpFlags op_flag, const std::string_view &op_name, const std::string_view &user_key,
                              const std::vector<std::string_view> &op_keys, int64_t *len);

        static bool GetBitFromValueAndOffset(const std::string &value, uint32_t offset);

        static bool IsEmptySegment(const Slice &segment);


    private:
        rocksdb::Status SubKeyScanner(RedisType type, const Slice &user_key, const Slice &cursor, uint64_t limit,
                                      const Slice &subkey_prefix, std::vector<std::string> *keys,
                                      std::vector<std::string> *values = nullptr);

        // string
        rocksdb::Status getValue(const std::string_view &ns_key, std::string *value);

        std::vector<rocksdb::Status> getValues(const std::vector<Slice> &ns_keys, std::vector<std::string> *values);

        rocksdb::Status getRawValue(const std::string_view &ns_key, std::string *raw_value);

        std::vector<rocksdb::Status> getRawValues(const std::vector<Slice> &keys, std::vector<std::string> *raw_values);

        rocksdb::Status updateRawValue(const Slice &ns_key, const std::string_view &raw_value);

        // list
        rocksdb::Status push(const Slice &user_key, const std::vector<std::string_view> &elems, bool create_if_missing, bool left,
                             int *ret);

        rocksdb::Status lmoveOnSingleList(const Slice &src, bool src_left, bool dst_left, std::string *elem);

        rocksdb::Status
        lmoveOnTwoLists(const Slice &src, const Slice &dst, bool src_left, bool dst_left, std::string *elem);

        rocksdb::Status GetBitmapMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value);
    protected:
        titandb::Storage *storage_;
        rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
        std::string namespace_;

        friend class LatestSnapShot;

        class LatestSnapShot {
        public:
            explicit LatestSnapShot(titandb::Storage *storage)
                    : storage_(storage), snapshot_(storage_->GetDB()->GetSnapshot()) {}

            ~LatestSnapShot() { storage_->GetDB()->ReleaseSnapshot(snapshot_); }

            const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }

            LatestSnapShot(const LatestSnapShot &) = delete;

            LatestSnapShot &operator=(const LatestSnapShot &) = delete;

        private:
            titandb::Storage *storage_ = nullptr;
            const rocksdb::Snapshot *snapshot_ = nullptr;
        };
    };

    class WriteBatchLogData {
    public:
        WriteBatchLogData() = default;

        explicit WriteBatchLogData(RedisType type) : type_(type) {}

        explicit WriteBatchLogData(RedisType type, std::vector<std::string> &&args) : type_(type),
                                                                                      args_(std::move(args)) {}

        RedisType GetRedisType();

        std::vector<std::string> *GetArguments();

        std::string Encode();

        turbo::Status Decode(const rocksdb::Slice &blob);

    private:
        RedisType type_ = kRedisNone;
        std::vector<std::string> args_;
    };

}  // namespace titandb
