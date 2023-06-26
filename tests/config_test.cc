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

#include <fstream>
#include <iostream>
#include <map>
#include <vector>

#include "titandb/commands/commander.h"
#include "config/config_util.h"
#include "server/server.h"

TEST(Config, GetAndSet) {
  const char *path = "test.conf";
  Config config;

  auto s = config.Load(CLIOptions(path));
  CHECK_FALSE(s.IsOK());
  std::map<std::string, std::string> mutable_cases = {
      {"timeout", "1000"},
      {"maxclients", "2000"},
      {"max-backup-to-keep", "1"},
      {"max-backup-keep-hours", "4000"},
      {"requirepass", "mytest_requirepass"},
      {"masterauth", "mytest_masterauth"},
      {"compact-cron", "1 2 3 4 5"},
      {"bgsave-cron", "5 4 3 2 1"},
      {"max-io-mb", "5000"},
      {"max-db-size", "6000"},
      {"max-replication-mb", "7000"},
      {"slave-serve-stale-data", "no"},
      {"slave-read-only", "no"},
      {"slave-priority", "101"},
      {"slowlog-log-slower-than", "1234"},
      {"slowlog-max-len", "123"},
      {"profiling-sample-ratio", "50"},
      {"profiling-sample-record-max-len", "1"},
      {"profiling-sample-record-threshold-ms", "50"},
      {"profiling-sample-commands", "get,set"},
      {"backup-dir", "test_dir/backup"},

      {"rocksdb.compression", "no"},
      {"rocksdb.max_open_files", "1234"},
      {"rocksdb.write_buffer_size", "1234"},
      {"rocksdb.max_write_buffer_number", "1"},
      {"rocksdb.target_file_size_base", "100"},
      {"rocksdb.max_background_compactions", "-1"},
      {"rocksdb.max_sub_compactions", "3"},
      {"rocksdb.delayed_write_rate", "1234"},
      {"rocksdb.stats_dump_period_sec", "600"},
      {"rocksdb.compaction_readahead_size", "1024"},
      {"rocksdb.level0_slowdown_writes_trigger", "50"},
      {"rocksdb.level0_stop_writes_trigger", "100"},
      {"rocksdb.enable_blob_files", "no"},
      {"rocksdb.min_blob_size", "4096"},
      {"rocksdb.blob_file_size", "268435456"},
      {"rocksdb.enable_blob_garbage_collection", "yes"},
      {"rocksdb.blob_garbage_collection_age_cutoff", "25"},
      {"rocksdb.max_bytes_for_level_base", "268435456"},
      {"rocksdb.max_bytes_for_level_multiplier", "10"},
      {"rocksdb.level_compaction_dynamic_level_bytes", "yes"},
      {"rocksdb.max_background_jobs", "4"},
  };
  std::vector<std::string> values;
  for (const auto &iter : mutable_cases) {
    s = config.Set(nullptr, iter.first, iter.second);
    REQUIRE(s.IsOK());
    config.Get(iter.first, &values);
    REQUIRE(s.IsOK());
    REQUIRE_EQ(values.size(), 2);
    CHECK_EQ(values[0], iter.first);
    CHECK_EQ(values[1], iter.second);
  }
  REQUIRE(config.Rewrite().IsOK());
  s = config.Load(CLIOptions(path));
  CHECK(s.IsOK());
  for (const auto &iter : mutable_cases) {
    s = config.Set(nullptr, iter.first, iter.second);
    REQUIRE(s.IsOK());
    config.Get(iter.first, &values);
    REQUIRE_EQ(values.size(), 2);
    CHECK_EQ(values[0], iter.first);
    CHECK_EQ(values[1], iter.second);
  }
  unlink(path);

  std::map<std::string, std::string> immutable_cases = {
      {"daemonize", "yes"},
      {"bind", "0.0.0.0"},
      {"repl-bind", "0.0.0.0"},
      {"workers", "8"},
      {"repl-workers", "8"},
      {"tcp-backlog", "500"},
      {"slaveof", "no one"},
      {"db-name", "test_dbname"},
      {"dir", "test_dir"},
      {"pidfile", "test.pid"},
      {"supervised", "no"},
      {"rocksdb.block_size", "1234"},
      {"rocksdb.max_background_flushes", "-1"},
      {"rocksdb.wal_ttl_seconds", "10000"},
      {"rocksdb.wal_size_limit_mb", "16"},
      {"rocksdb.enable_pipelined_write", "no"},
      {"rocksdb.cache_index_and_filter_blocks", "no"},
      {"rocksdb.metadata_block_cache_size", "100"},
      {"rocksdb.subkey_block_cache_size", "100"},
      {"rocksdb.row_cache_size", "100"},
      {"rocksdb.rate_limiter_auto_tuned", "yes"},
  };
  for (const auto &iter : immutable_cases) {
    s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_FALSE(s.IsOK());
  }
}

TEST(Config, GetRenameCommand) {
  const char *path = "test.conf";
  unlink(path);

  std::ofstream output_file(path, std::ios::out);
  output_file << "rename-command KEYS KEYS_NEW"
              << "\n";
  output_file << "rename-command GET GET_NEW"
              << "\n";
  output_file << "rename-command SET SET_NEW"
              << "\n";
  output_file.close();
  redis::ResetCommands();
  Config config;
  REQUIRE(config.Load(CLIOptions(path)).IsOK());
  std::vector<std::string> values;
  config.Get("rename-command", &values);
  REQUIRE_EQ(values[1], "KEYS KEYS_NEW");
  REQUIRE_EQ(values[3], "GET GET_NEW");
  REQUIRE_EQ(values[5], "SET SET_NEW");
  REQUIRE_EQ(values[0], "rename-command");
  REQUIRE_EQ(values[2], "rename-command");
  REQUIRE_EQ(values[4], "rename-command");
}

TEST(Config, Rewrite) {
  const char *path = "test.conf";
  unlink(path);

  std::ofstream output_file(path, std::ios::out);
  output_file << "rename-command KEYS KEYS_NEW"
              << "\n";
  output_file << "rename-command GET GET_NEW"
              << "\n";
  output_file << "rename-command SET SET_NEW"
              << "\n";
  output_file.close();

  redis::ResetCommands();
  Config config;
  REQUIRE(config.Load(CLIOptions(path)).IsOK());
  REQUIRE(config.Rewrite().IsOK());
  // Need to re-populate the command table since it has renamed by the previous
  redis::ResetCommands();
  Config new_config;
  REQUIRE(new_config.Load(CLIOptions(path)).IsOK());
  unlink(path);
}

TEST(Namespace, Add) {
  const char *path = "test.conf";
  unlink(path);

  Config config;
  auto s = config.Load(CLIOptions(path));
  CHECK_FALSE(s.IsOK());
  config.slot_id_encoded = false;
  CHECK(!config.AddNamespace("ns", "t0").IsOK());
  config.requirepass = "foobared";

  std::vector<std::string> namespaces = {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for (size_t i = 0; i < namespaces.size(); i++) {
    CHECK(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for (size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    s = config.GetNamespace(namespaces[i], &token);
    CHECK(s.IsOK());
    CHECK_EQ(token, tokens[i]);
  }
  for (size_t i = 0; i < namespaces.size(); i++) {
    s = config.AddNamespace(namespaces[i], tokens[i]);
    CHECK_FALSE(s.IsOK());
    CHECK_EQ(s.Msg(), "the token has already exists");
  }
  s = config.AddNamespace("n1", "t0");
  CHECK_FALSE(s.IsOK());
  CHECK_EQ(s.Msg(), "the namespace has already exists");

  s = config.AddNamespace(kDefaultNamespace, "mytoken");
  CHECK_FALSE(s.IsOK());
  CHECK_EQ(s.Msg(), "forbidden to add the default namespace");
  unlink(path);
}

TEST(Namespace, Set) {
  const char *path = "test.conf";
  unlink(path);

  Config config;
  auto s = config.Load(CLIOptions(path));
  CHECK_FALSE(s.IsOK());
  config.slot_id_encoded = false;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces = {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  std::vector<std::string> new_tokens = {"nt1", "nt2'", "nt3", "nt4"};
  for (size_t i = 0; i < namespaces.size(); i++) {
    s = config.SetNamespace(namespaces[i], tokens[i]);
    CHECK_FALSE(s.IsOK());
    CHECK_EQ(s.Msg(), "the namespace was not found");
  }
  for (size_t i = 0; i < namespaces.size(); i++) {
    CHECK(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for (size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    s = config.GetNamespace(namespaces[i], &token);
    CHECK(s.IsOK());
    CHECK_EQ(token, tokens[i]);
  }
  for (size_t i = 0; i < namespaces.size(); i++) {
    CHECK(config.SetNamespace(namespaces[i], new_tokens[i]).IsOK());
  }
  for (size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    s = config.GetNamespace(namespaces[i], &token);
    CHECK(s.IsOK());
    CHECK_EQ(token, new_tokens[i]);
  }
  unlink(path);
}

TEST(Namespace, Delete) {
  const char *path = "test.conf";
  unlink(path);

  Config config;
  auto s = config.Load(CLIOptions(path));
  CHECK_FALSE(s.IsOK());
  config.slot_id_encoded = false;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces = {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for (size_t i = 0; i < namespaces.size(); i++) {
    CHECK(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for (size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    s = config.GetNamespace(namespaces[i], &token);
    CHECK(s.IsOK());
    CHECK_EQ(token, tokens[i]);
  }
  for (const auto &ns : namespaces) {
    s = config.DelNamespace(ns);
    CHECK(s.IsOK());
    std::string token;
    s = config.GetNamespace(ns, &token);
    CHECK_FALSE(s.IsOK());
    CHECK(token.empty());
  }
  unlink(path);
}

TEST(Namespace, RewriteNamespaces) {
  const char *path = "test.conf";
  unlink(path);
  Config config;
  auto s = config.Load(CLIOptions(path));
  CHECK_FALSE(s.IsOK());
  config.requirepass = "test";
  config.backup_dir = "test";
  config.slot_id_encoded = false;
  std::vector<std::string> namespaces = {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for (size_t i = 0; i < namespaces.size(); i++) {
    CHECK(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  CHECK(config.AddNamespace("to-be-deleted-ns", "to-be-deleted-token").IsOK());
  CHECK(config.DelNamespace("to-be-deleted-ns").IsOK());

  Config new_config;
  s = new_config.Load(CLIOptions(path));
  CHECK(s.IsOK());
  for (size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    s = new_config.GetNamespace(namespaces[i], &token);
    CHECK(s.IsOK());
    CHECK_EQ(token, tokens[i]);
  }

  std::string token;
  CHECK_FALSE(new_config.GetNamespace("to-be-deleted-ns", &token).IsOK());
  unlink(path);
}

TEST(Config, ParseConfigLine) {
  REQUIRE_EQ(*ParseConfigLine(""), ConfigKV{});
  REQUIRE_EQ(*ParseConfigLine("# hello"), ConfigKV{});
  REQUIRE_EQ(*ParseConfigLine("       #x y z "), ConfigKV{});
  REQUIRE_EQ(*ParseConfigLine("key value  "), (ConfigKV{"key", "value"}));
  REQUIRE_EQ(*ParseConfigLine("key value#x"), (ConfigKV{"key", "value"}));
  REQUIRE_EQ(*ParseConfigLine("key"), (ConfigKV{"key", ""}));
  REQUIRE_EQ(*ParseConfigLine("    key    value1   value2   "), (ConfigKV{"key", "value1   value2"}));
  REQUIRE_EQ(*ParseConfigLine(" #"), ConfigKV{});
  REQUIRE_EQ(*ParseConfigLine("  key val ue #h e l l o"), (ConfigKV{"key", "val ue"}));
  REQUIRE_EQ(*ParseConfigLine("key 'val ue'"), (ConfigKV{"key", "val ue"}));
  REQUIRE_EQ(*ParseConfigLine(R"(key ' value\'\'v a l ')"), (ConfigKV{"key", " value''v a l "}));
  REQUIRE_EQ(*ParseConfigLine(R"( key "val # hi" # hello!)"), (ConfigKV{"key", "val # hi"}));
  REQUIRE_EQ(*ParseConfigLine(R"(key "\n \r \t ")"), (ConfigKV{"key", "\n \r \t "}));
  REQUIRE_EQ(*ParseConfigLine("key ''"), (ConfigKV{"key", ""}));
  ASSERT_FALSE(ParseConfigLine("key \"hello "));
  ASSERT_FALSE(ParseConfigLine("key \'\\"));
  ASSERT_FALSE(ParseConfigLine("key \"hello'"));
  ASSERT_FALSE(ParseConfigLine("key \""));
  ASSERT_FALSE(ParseConfigLine("key '' ''"));
  ASSERT_FALSE(ParseConfigLine("key '' x"));
}

TEST(Config, DumpConfigLine) {
  REQUIRE_EQ(DumpConfigLine({"key", "value"}), "key value");
  REQUIRE_EQ(DumpConfigLine({"key", " v a l "}), R"(key " v a l ")");
  REQUIRE_EQ(DumpConfigLine({"a", "'b"}), "a \"\\'b\"");
  REQUIRE_EQ(DumpConfigLine({"a", "x#y"}), "a \"x#y\"");
  REQUIRE_EQ(DumpConfigLine({"a", "x y"}), "a \"x y\"");
  REQUIRE_EQ(DumpConfigLine({"a", "xy"}), "a xy");
  REQUIRE_EQ(DumpConfigLine({"a", "x\n"}), "a \"x\\n\"");
}
