#include <gtest/gtest.h>
#include "sstable.hpp"
#include "db_entry.hpp"
#include <cstdio>
#include <string>
#include <map>
#include <filesystem>

TEST(SSTableBigDataTest, WriteAndFindManyRecords) {
    const std::string test_filename = "test_sstable_big.txt";
    std::map<std::string, DB::DBEntry> data;
    const int num_records = 10000;

    for (int i = 0; i < num_records; ++i) {
        data["key_" + std::to_string(i)] = {"value_" + std::to_string(i), false};
    }

    DB::SSTable sstable(test_filename);
    sstable.write(data);

    DB::DBEntry result;
    for (int i = 0; i < num_records; ++i) {
        bool found = sstable.find("key_" + std::to_string(i), result);
        EXPECT_TRUE(found);
        EXPECT_EQ(result.value, "value_" + std::to_string(i));
        EXPECT_FALSE(result.tombstone);
    }

    std::filesystem::remove(test_filename);
}

TEST(SSTableBigDataTest, DumpManyRecords) {
    const std::string test_filename = "test_sstable_dump.txt";
    std::map<std::string, DB::DBEntry> data;
    const int num_records = 10000;

    for (int i = 0; i < num_records; ++i) {
        data["key_" + std::to_string(i)] = {"value_" + std::to_string(i), false};
    }

    DB::SSTable sstable(test_filename);
    sstable.write(data);

    auto all_records = sstable.dump();
    EXPECT_EQ(all_records.size(), num_records);
    for (int i = 0; i < num_records; ++i) {
        std::string key = "key_" + std::to_string(i);
        EXPECT_EQ(all_records[key].value, "value_" + std::to_string(i));
        EXPECT_FALSE(all_records[key].tombstone);
  }

    std::filesystem::remove(test_filename);
}  
