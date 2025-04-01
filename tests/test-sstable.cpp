#include <userver/utest/utest.hpp>
#include <filesystem>
#include "sstable.hpp"

namespace fs = std::filesystem;
using namespace DB;

const std::string kTestFilename = "test_sstable.dat";

UTEST(SSTableTest, WriteAndFind) {
    fs::remove(kTestFilename);

    userver::engine::RunCoro([&]() {
        SSTable sstable(kTestFilename, 0);
        std::map<std::string, std::optional<std::string>> data = {
            {"key1", "value1"},
            {"key2", "value2"},
            {"key3", std::nullopt}
        };
        sstable.write(data);
        std::optional<std::string> value;
        
        EXPECT_TRUE(sstable.find("key1", value));
        EXPECT_EQ(value, "value1");

        EXPECT_TRUE(sstable.find("key2", value));
        EXPECT_EQ(value, "value2");

        EXPECT_TRUE(sstable.find("key3", value));
        EXPECT_FALSE(value.has_value()); 

        EXPECT_FALSE(sstable.find("unknown", value));
    });
}

UTEST(SSTableTest, IndexRecovery) {
    fs::remove(kTestFilename);

    userver::engine::RunCoro([&]() {
        {
            SSTable sstable(kTestFilename, 0);
            std::map<std::string, std::optional<std::string>> data = {
                {"a", "apple"},
                {"b", "banana"},
            };
            sstable.write(data);
        }

        SSTable sstable(kTestFilename, 0);
        std::optional<std::string> value;

        EXPECT_TRUE(sstable.find("a", value));
        EXPECT_EQ(value, "apple");

        EXPECT_TRUE(sstable.find("b", value));
        EXPECT_EQ(value, "banana");

        EXPECT_FALSE(sstable.find("c", value)); 
    });
}

UTEST(SSTableTest, DumpTest) {
    fs::remove(kTestFilename);

    userver::engine::RunCoro([&]() {
        SSTable sstable(kTestFilename, 0);
        std::map<std::string, std::optional<std::string>> data = {
            {"x", "xyz"},
            {"y", std::nullopt},
            {"z", "zzz"}
        };
        sstable.write(data);

        auto dumpedData = sstable.dump();

        EXPECT_EQ(dumpedData["x"], "xyz");
        EXPECT_EQ(dumpedData["z"], "zzz");
        EXPECT_FALSE(dumpedData["y"].has_value());
    });
}
