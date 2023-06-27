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

#ifndef TITANDB_DB_H_
#define TITANDB_DB_H_

#include <memory>
#include <vector>

#include "titandb/config.h"
#include "titandb/storage/storage.h"
#include "turbo/base/result_status.h"
#include "titandb/redis_db.h"

namespace titandb {

    class TitanDB {
    public:
        TitanDB() = default;

        virtual ~TitanDB() = default;

        explicit TitanDB(Storage *s, const std::string_view &ns = kDefaultNamespace);

        turbo::Status init(Storage *s, const std::string_view &ns = kDefaultNamespace);

        static turbo::ResultStatus<Storage *> CreateStorage(Config *config);

        // strings
        ///
        /// @brief Get the value of key. If the key does not exist
        ///        the special value nil is returned
        /// \param key index
        /// \return
        turbo::ResultStatus<std::string> Get(const std::string_view &key);

        turbo::ResultStatus<std::string> GetEx(const std::string_view &key, int ttl = 0);

        ///
        /// \brief Returns the length of the string value stored at key. An error
        ///        is returned when key holds a non-string value.
        /// \param key
        /// \return
        turbo::ResultStatus<size_t> Strlen(const std::string_view &key);

        ///
        /// \brief Atomically sets key to value and returns the old value stored at key
        ///        Returns an error when key exists but does not hold a string value.
        /// \param key
        /// \param new_value
        /// \return
        turbo::ResultStatus<std::string> GetSet(const std::string_view &key, const std::string_view &new_value);

        ///
        /// \brief Get the value of key and del the key and value. If the key does not exist
        ///        the special value nil is returned
        /// \param key
        /// \return
        turbo::ResultStatus<std::string> GetDel(const std::string_view &key);

        ///
        /// \brief Returns the substring of the string value stored at key,
        ///        determined by the offsets start and end (both are inclusive)
        /// \param key
        /// \param start
        /// \param stop
        /// \return
        turbo::ResultStatus<std::string>
        GetRange(const std::string_view &key, int start = 0, int stop = std::numeric_limits<int>::max());

        ///
        /// \brief Returns the substring of the string value stored at key,
        ///        determined by the offsets start and end (both are inclusive)
        /// \param key
        /// \param start
        /// \param stop
        /// \return
        turbo::ResultStatus<std::string>
        SubStr(const std::string_view &key, int start = 0, int stop = std::numeric_limits<int>::max());

        ///
        /// \brief Set key to hold string value if key does not exist
        ///        return the length of the string after it was modified by the command
        /// \param key
        /// \param offset
        /// \param value
        /// \return
        turbo::ResultStatus<int> SetRange(const std::string_view &key, size_t offset, const std::string_view &value);

        ///
        /// \brief Returns the values of all specified keys. For every key
        ///        that does not hold a string value or does not exist, the
        ///        special value nil is returned
        /// \param keys
        /// \return
        std::vector<turbo::ResultStatus<std::string>> MGet(const std::vector<std::string_view> &keys);

        ///
        /// \brief If key already exists and is a string, this command appends the value at
        ///        the end of the string
        ///        return the length of the string after the append operation
        /// \param key
        /// \param value
        /// \return
        turbo::ResultStatus<size_t> Append(const std::string_view &key, const std::string_view &value);

        ///
        /// \brief Set key to hold the string value. if key already holds a value, it is overwritten
        /// \param key index key
        /// \param value data field
        /// \return
        turbo::Status Set(const std::string_view &key, const std::string_view &value);

        ///
        /// \brief Set key to hold the string value and set key to timeout after a given
        ///        number of seconds
        /// \param key
        /// \param value
        /// \param ttl
        /// \return
        turbo::Status SetEx(const std::string_view &key, const std::string_view &value, int ttl);

        ///
        /// \brief PSetEx has the same effect and semantic as @SetEx, but instead of
        ///        specifying the number of milliseconds representing the TTL (time to live).
        /// \param key
        /// \param value
        /// \param ttl
        /// \return
        turbo::Status PSetEx(const std::string_view &key, const std::string_view &value, int64_t ttl);

        ///
        /// @brief Set key to hold string value if key does not exist,return 1 if the key was set
        ///        return 0 if the key was not set
        /// \param key
        /// \param value
        /// \return
        turbo::ResultStatus<int> SetNx(const std::string_view &key, const std::string_view &value);

        ///
        /// \brief Sets the given keys to their respective values.
        ///        MSETNX will not perform any operation at all even
        ///         if just a single key already exists.
        /// \param kvs key and value pairs
        /// \return
        turbo::ResultStatus<int> MSetNx(const std::vector<StringPair> &kvs);

        ///
        /// \brief Sets the given keys to their respective values
        ///        MSET replaces existing values with new values
        /// \param kvs key and value pairs
        /// \return number saved
        turbo::ResultStatus<int> MSet(const std::vector<StringPair> &kvs);

        ///
        /// \brief Increments the number stored at field in the hash stored at key by
        ///        increment. If key does not exist, a new key holding a hash is created. If
        ///        field does not exist the value is set to 0 before the operation is
        ///        performed.
        /// \param key
        /// \param value
        /// \return
        turbo::ResultStatus<int64_t> IncrBy(const std::string_view &key, int64_t value);

        ///
        /// \brief Increment the specified field of a hash stored at key, and representing a
        ///        floating point number, by the specified increment. If the increment value
        ///        is negative, the result is to have the hash field value decremented instead
        ///        of incremented. If the field does not exist, it is set to 0 before
        ///        performing the operation. An error is returned if one of the following
        ///        conditions occur:
        ///
        ///        The field contains a value of the wrong type (not a string).
        ///        The current field content or the specified increment are not parsable as a
        ///        double precision floating point number.
        /// \param key
        /// \param value
        /// \return
        turbo::ResultStatus<double> IncrByFloat(const std::string_view &key, double value);

        ///
        /// \brief Increments 1 stored at field in the hash stored at key by
        ///        increment. If key does not exist, a new key holding a hash is created. If
        ///        field does not exist the value is set to 0 before the operation is
        ///        performed.
        /// \param key
        /// \param value
        /// \return

        turbo::ResultStatus<int64_t> Incr(const std::string_view &key);

        ///
        /// \brief Increments the -1 * value stored at field in the hash stored at key by
        ///        increment. If key does not exist, a new key holding a hash is created. If
        ///        field does not exist the value is set to 0 before the operation is
        ///        performed.
        /// \param key
        /// \param value
        /// \return
        turbo::ResultStatus<int64_t> DecrBy(const std::string_view &key, int64_t value);

        ///
        /// \brief Increments the -1 stored at field in the hash stored at key by
        ///        increment. If key does not exist, a new key holding a hash is created. If
        ///        field does not exist the value is set to 0 before the operation is
        ///        performed.
        /// \param key
        /// \param value
        /// \return
        turbo::ResultStatus<int64_t> Decr(const std::string_view &key);

        turbo::ResultStatus<int>
        CAS(const std::string_view &key, const std::string_view &old_value, std::string_view &new_value, int ttl = 0);

        turbo::ResultStatus<int> CAD(const std::string_view &key, const std::string_view &value);

        //Bit map

        ///
        /// \brief Returns the bit value at offset in the string value stored at key
        /// \param key
        /// \param offset
        /// \return
        turbo::ResultStatus<bool> GetBit(const std::string_view &key, uint32_t offset);

        ///
        /// \brief Sets or clears the bit at offset in the string value stored at key
        /// \param key
        /// \param offset
        /// \param flag
        /// \return
        turbo::ResultStatus<bool> SetBit(const std::string_view &key, uint32_t offset, bool flag);

        ///
        /// \brief Count the number of set bits (population counting) in a string.
        ///        return the number of bits set to 1
        ///        note: if need to specified offset, set have_range to true
        /// \param key
        /// \param start
        /// \param stop
        /// \return
        turbo::ResultStatus<uint32_t> BitCount(const std::string_view &key, int64_t start, int64_t stop = -1);

        turbo::ResultStatus<int64_t> BitPos(const std::string_view &key, bool flag, int64_t start, int64_t stop);

        turbo::ResultStatus<int64_t> BitOP(BitOpFlags op, const std::string_view &dest_key, const std::string_view &key,
                                           const std::vector<std::string_view> &keys);

        // hash

        turbo::ResultStatus<std::string> HGet(const std::string_view &key, const std::string_view &field);

        turbo::ResultStatus<int64_t> HIncrBy(const std::string_view &key, const std::string_view &field, int64_t value);

        turbo::ResultStatus<double>
        HIncrByFloat(const std::string_view &key, const std::string_view &field, double value);

        turbo::ResultStatus<int>
        HSet(const std::string_view &key, const std::string_view &field, const std::string_view &value);

        turbo::Status HSetNx(const std::string_view &key, const std::vector<FieldValue> &kvs);

        turbo::ResultStatus<int> HDel(const std::string_view &key, const std::vector<std::string_view> &fields);

        turbo::ResultStatus<size_t> HStrlen(const std::string_view &key, const std::string_view &field);

        turbo::ResultStatus<bool> HExists(const std::string_view &key, const std::string_view &field);

        turbo::ResultStatus<size_t> HLen(const std::string_view &key);

        turbo::ResultStatus<std::vector<turbo::ResultStatus<std::string>>>
        HMGet(const std::string_view &key, const std::vector<std::string_view> &fields);

        turbo::Status HMSet(const std::string_view &key, const std::vector<FieldValue> &kvs);

        turbo::ResultStatus<std::vector<std::string>> HKeys(const std::string_view &key);

        turbo::ResultStatus<std::vector<std::string>> HVals(const std::string_view &key);

        turbo::ResultStatus<std::vector<FieldValue>> HGetAll(const std::string_view &key);

        turbo::ResultStatus<FieldValueArray>
        HScan(const std::string_view &key, const std::string_view &cursor, const std::string_view &prefix,
              uint64_t limits);

        turbo::ResultStatus<std::vector<FieldValue>> HRangeByLex(const std::string_view &key, RangeLexSpec spec);

        // key
        turbo::ResultStatus<int64_t> TTL(const std::string_view &key);

        turbo::ResultStatus<int64_t> PTTL(const std::string_view &key);

        turbo::ResultStatus<RedisType> Type(const std::string_view &key);

        turbo::ResultStatus<std::vector<std::string>> Object(const std::string_view &key);

        turbo::ResultStatus<int> Exists(const std::vector<std::string_view> &keys);

        turbo::ResultStatus<int> Persist(const std::string_view &key);

        turbo::ResultStatus<int> Expire(const std::string_view &key, int ttl);

        turbo::ResultStatus<int> PExpire(const std::string_view &key, int64_t ttl);

        turbo::ResultStatus<int> ExpireAt(const std::string_view &key, int ttl);

        turbo::ResultStatus<int> PExpireAt(const std::string_view &key, int64_t ttl);

        turbo::ResultStatus<int> Del(const std::string_view &key);

        turbo::ResultStatus<int> Unlink(const std::string_view &key);

        // list
        turbo::ResultStatus<int> LPush(const std::string_view &key, const std::vector<std::string_view> &items);

        turbo::ResultStatus<int> RPush(const std::string_view &key, const std::vector<std::string_view> &items);

        turbo::ResultStatus<int> LPushX(const std::string_view &key, const std::vector<std::string_view> &items);

        turbo::ResultStatus<int> RPushX(const std::string_view &key, const std::vector<std::string_view> &items);

        turbo::ResultStatus<std::string> LPop(const std::string_view &key);

        turbo::ResultStatus<std::vector<std::string>> LPop(const std::string_view &key, int32_t count);

        turbo::ResultStatus<std::string> RPop(const std::string_view &key);

        turbo::ResultStatus<std::vector<std::string>> RPop(const std::string_view &key, int32_t count);

        turbo::ResultStatus<int> LRem(const std::string_view &key, const std::string_view &elem, uint32_t count);

        turbo::ResultStatus<int>
        LInsert(const std::string_view &key, const std::string_view &pov, const std::string_view &elm, bool before);

        turbo::ResultStatus<std::vector<std::string>> LRange(const std::string_view &key, int start, int stop);

        turbo::ResultStatus<std::string> LIndex(const std::string_view &key, int index);

        turbo::Status LTrim(const std::string_view &key, int start, int stop);

        turbo::Status LSet(const std::string_view &key, const std::string_view &value, int index);

        turbo::ResultStatus<std::string> RPopPush(const std::string_view &src, const std::string_view &dst);

        turbo::ResultStatus<std::string>
        LMove(const std::string_view &src, const std::string_view &dst, bool src_left, bool dst_left);

        // set
        turbo::ResultStatus<int> SAdd(const std::string_view &key, const std::vector<std::string_view> &elems);

        turbo::ResultStatus<int> SRem(const std::string_view &key, const std::vector<std::string_view> &elems);

        turbo::ResultStatus<int> SCard(const std::string_view &key);

        turbo::ResultStatus<std::vector<std::string>> SMembers(const std::string_view &key);

        turbo::ResultStatus<bool> SisMembers(const std::string_view &key, const std::string_view &value);

        turbo::ResultStatus<std::vector<int>>
        SmisMembers(const std::string_view &key, const std::vector<std::string_view> &elems);

        turbo::ResultStatus<std::vector<std::string>> SPop(const std::string_view &key, int count = 1);

        turbo::ResultStatus<std::vector<std::string>> SRandMember(const std::string_view &key, int count = 1);

        turbo::ResultStatus<int>
        SMove(const std::string_view &src, const std::string_view &dst, const std::string_view &member);

        turbo::ResultStatus<std::vector<std::string>> SDiff(const std::vector<std::string_view> &keys);

        turbo::ResultStatus<std::vector<std::string>> SUnion(const std::vector<std::string_view> &keys);

        turbo::ResultStatus<std::vector<std::string>> SInter(const std::vector<std::string_view> &keys);

        turbo::ResultStatus<int> SDiffStore(const std::string_view &dst, const std::vector<std::string_view> &keys);

        turbo::ResultStatus<int> SUnionStore(const std::string_view &st, const std::vector<std::string_view> &keys);

        turbo::ResultStatus<int> SInterStore(const std::string_view &dst, const std::vector<std::string_view> &keys);

        turbo::ResultStatus<std::vector<std::string>>
        SScan(const std::string_view &user_key, const std::string_view &cursor, uint64_t limit,
              const std::string &member_prefix);


        // si
        turbo::ResultStatus<int> SIAdd(const std::string_view &key, const std::vector<uint64_t> &ids);

        turbo::ResultStatus<int> SIRem(const std::string_view &key, const std::vector<uint64_t> &ids);

        turbo::ResultStatus<int> SICard(const std::string_view &key);

        turbo::ResultStatus<std::vector<int>> SIExists(const std::string_view &key, const std::vector<uint64_t> &ids);

        turbo::ResultStatus<std::vector<uint64_t>>
        SIRange(const Slice &user_key, uint64_t cursor_id, uint64_t page, uint64_t limit);

        turbo::ResultStatus<std::vector<uint64_t>>
        SIRevRange(const Slice &user_key, uint64_t cursor_id, uint64_t page, uint64_t limit);

        turbo::ResultStatus<std::vector<uint64_t>>
        SIRangeByValue(const std::string_view &key, const SortedintRangeSpec &spec);

        // db
        turbo::Status BeginTxn();

        turbo::Status CommitTxn();


    private:
        Storage *_storage{nullptr};
        std::string _ns;
        const Config *_config;
        std::unique_ptr<RedisDB> _db;
    };
}  // namespace titandb

#endif  // TITANDB_DB_H_
