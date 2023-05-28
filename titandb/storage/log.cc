//
// Created by ubuntu on 23-5-26.
//

#include "titandb/storage/log.h"
#include "titandb/storage/storage.h"

namespace titandb {
    std::string ServerLogData::Encode() {
        if (type_ == kReplIdLog) {
            return std::string(1, kReplIdTag) + " " + content_;
        }
        return content_;
    }

    turbo::Status ServerLogData::Decode(const rocksdb::Slice &blob) {
        if (blob.size() == 0) {
            return turbo::UnavailableError("");
        }

        const char *header = blob.data();
        // Only support `kReplIdTag` now
        if (*header == kReplIdTag && blob.size() == 2 + kReplIdLength) {
            type_ = kReplIdLog;
            content_ = std::string(blob.data() + 2, blob.size() - 2);
            return turbo::OkStatus();
        }
        return turbo::UnavailableError("");
    }

}  // namespace titandb
