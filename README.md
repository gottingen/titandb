# titandb

titandb is a disk redis engine based on rocksdb and I make it wisc -key an provide redis interface.

**build**
```shell
git clone https://github.com/gottingen/titandb.git
carbin install
mkdir build
cd build
cmake ..
make
```

# install

```shell
conda install -c titan-search titandb
```

# example
```c++

#include <thread>
#include <memory>
#include "titandb/db.h"
#include "turbo/format/fmt/printf.h"
#include "turbo/files/filesystem.h"

int main() {

    std::unique_ptr<titandb::Config> config(new titandb::Config());
    config->db_dir = "testdb";
    config->backup_dir = "testdb/backup";
    config->rocks_db.compression = rocksdb::CompressionType::kNoCompression;
    config->rocks_db.write_buffer_size = 1;
    config->rocks_db.block_size = 100;
    std::unique_ptr<titandb::Storage> storage(new titandb::Storage(config.get()));
    auto ss = storage->Open();
    if (ss.ok()) {
        printf("Open success\n");
    } else {
        printf("Open failed, error: %s\n", ss.ToString().c_str());
        return -1;
    }
    titandb::TitanDB db(storage.get(), "th");
    // HSet

    auto s = db.HSet("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1");
    fmt::printf("HSet return: %s, res = %d\n", s.status().ToString(), s.value());
    s = db.HSet("TEST_KEY1", "TEST_FIELD2", "TEST_VALUE2");
    fmt::printf("HSet return: %s, res = %d\n", s.status().ToString(), s.value());

    s = db.HSet("TEST_KEY2", "TEST_FIELD1", "TEST_VALUE1");
    fmt::printf("HSet return: %s, res = %d\n", s.status().ToString(), s.value());
    s = db.HSet("TEST_KEY2", "TEST_FIELD2", "TEST_VALUE2");
    fmt::printf("HSet return: %s, res = %d\n", s.status().ToString(), s.value());
    s = db.HSet("TEST_KEY2", "TEST_FIELD3", "TEST_VALUE3");
    fmt::printf("HSet return: %s, res = %d\n", s.status().ToString(), s.value());

    // HGet
    auto gs = db.HGet("TEST_KEY1", "TEST_FIELD1");
    fmt::printf("HGet return: %s, value = %s\n", gs.status().ToString(), gs.value());
    gs = db.HGet("TEST_KEY1", "TEST_FIELD2");
    fmt::printf("HGet return: %s, value = %s\n", gs.status().ToString(), gs.value());
    gs = db.HGet("TEST_KEY1", "TEST_FIELD3");
    fmt::printf("HGet return: %s, error: %s", gs.ok() ? "found" : "not found", gs.status().ToString());
    if(gs.ok()) {
        fmt::printf("HGet value: %s\n", gs.value());
    }
    gs = db.HGet("TEST_KEY_NOT_EXIST", "TEST_FIELD");
    fmt::printf("HGet return: %s, error: %s", gs.ok() ? "found" : "not found", gs.status().ToString());
    if(gs.ok()) {
        fmt::printf("HGet value: %s\n", gs.value());
    }

    // HMSet
    std::vector<titandb::FieldValue> fvs;
    fvs.push_back({"TEST_FIELD1", "TEST_VALUE1"});
    fvs.push_back({"TEST_FIELD2", "TEST_VALUE2"});
    auto ts = db.HMSet("TEST_HASH", fvs);
    fmt::printf("HMset return: %s\n", ts.ToString().c_str());

    // HMGet

    std::vector<std::string_view> fields;
    fields.push_back("TEST_FIELD1");
    fields.push_back("TEST_FIELD2");
    auto mrs = db.HMGet("TEST_HASH", fields);
    fmt::printf("HMget return: %s\n", s.status().ToString().c_str());
    if(mrs.ok()) {
        auto &vs = mrs.value();
        for (uint32_t idx = 0; idx != fields.size(); idx++) {
            fmt::printf("idx = %d, field = %s, value = %s\n",
                   idx, fields[idx], vs[idx].ok() ? vs[idx].value() : "not found");
        }
    }

    // HLEN
    auto hls = db.HLen("TEST_HASH");
    fmt::printf("HLen return : %s, len = %lu\n", hls.status().ToString(), hls.ok() ? hls.value() : 0ul);

    std::error_code ec;
    if(turbo::filesystem::exists("testdb")) {
        turbo::filesystem::remove_all("testdb");
    }
    return 0;
}

```