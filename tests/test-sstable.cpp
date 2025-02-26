#include <userver/utest/utest.hpp>
#include "database.hpp"
#include <fstream>

namespace DB {

    class SSTableTest : public ::testing::Test {
    protected:
        void SetUp() override {
            std::ofstream file("sstable_test.txt");
            file << "key1 value1\nkey2 value2\nkey3 value3\n";
            file.close();
        }

        void TearDown() override {
            std::remove("sstable_test.txt");
        }
    };

    UTEST_F(SSTableTest, ReadExistingKey) {
        SSTable sstable("sstable_test.txt");
        EXPECT_EQ(sstable.read("key1"), "value1");
        EXPECT_EQ(sstable.read("key2"), "value2");
        EXPECT_EQ(sstable.read("key3"), "value3");
    }

    UTEST_F(SSTableTest, ReadNonExistingKey) {
        SSTable sstable("sstable_test.txt");
        EXPECT_EQ(sstable.read("key4"), "");
    }

    UTEST_F(SSTableTest, WriteAndRead) {
        SSTable sstable("sstable_test.txt");

        std::map<std::string, std::string> data = {
            {"key4", "value4"},
            {"key5", "value5"},
            {"key6", "value6"}
        };

        sstable.write(data);

        EXPECT_EQ(sstable.read("key4"), "value4");
        EXPECT_EQ(sstable.read("key5"), "value5");
        EXPECT_EQ(sstable.read("key6"), "value6");
    }

    UTEST_F(SSTableTest, DataIsSortedAfterWrite) {
        SSTable sstable("sstable_test.txt");

        std::map<std::string, std::string> unsorted_data = {
            {"key3", "value3"},
            {"key1", "value1"},
            {"key4", "value4"},
            {"key2", "value2"}
        };

        sstable.write(unsorted_data);

        std::ifstream file("sstable_test.txt");
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(file, line)) {
            lines.push_back(line);
        }

        std::vector<std::string> expected = {
            "key1 value1", "key2 value2", "key3 value3", "key4 value4"
        };

        EXPECT_EQ(lines, expected);
    }

}  // namespace DB
