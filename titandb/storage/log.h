//
// Created by ubuntu on 23-5-26.
//

#ifndef TEDIS_LOG_H
#define TEDIS_LOG_H

#include <string>
#include "turbo/base/status.h"
#include "rocksdb/db.h"

namespace titandb {
    enum ServerLogType {
        kServerLogNone, kReplIdLog
    };

    class ServerLogData {
    public:
        // Redis::WriteBatchLogData always starts with digit ascii, we use alphabetic to
        // distinguish ServerLogData with Redis::WriteBatchLogData.
        static const char kReplIdTag = 'r';

        static bool IsServerLogData(const char *header) {
            if (header) return *header == kReplIdTag;
            return false;
        }

        ServerLogData() = default;

        explicit ServerLogData(ServerLogType type, std::string content) : type_(type), content_(std::move(content)) {}

        ServerLogType GetType() { return type_; }

        std::string GetContent() { return content_; }

        std::string Encode();

        turbo::Status Decode(const rocksdb::Slice &blob);

    private:
        ServerLogType type_ = kServerLogNone;
        std::string content_;
    };
}  // namespace titandb

#endif //TEDIS_LOG_H
