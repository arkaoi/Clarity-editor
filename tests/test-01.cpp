#include <userver/utest/utest.hpp>
#include "db_base.hpp"
#include <fstream>

namespace DB {

    class DatabaseTest : public ::testing::Test {
    protected:
        void SetUp() override {
            std::ofstream file("test_db.txt");
            file << "key1 value1\nkey2 value2\n";
            file.close();
        }

        void TearDown() override {
            std::remove("test_db.txt");
        }
    };

    UTEST_F(DatabaseTest, LoadFromFile) {
        Database db("test_db.txt");
        EXPECT_NE(db.select("key1"), nullptr);
        EXPECT_EQ(*db.select("key1"), "value1");
        EXPECT_NE(db.select("key2"), nullptr);
        EXPECT_EQ(*db.select("key2"), "value2");
    }

    UTEST_F(DatabaseTest, InsertAndSelect) {
        Database db("test_db.txt");
        db.insert("key3", "value3");
        EXPECT_NE(db.select("key3"), nullptr);
        EXPECT_EQ(*db.select("key3"), "value3");
    }

    UTEST_F(DatabaseTest, Remove) {
        Database db("test_db.txt");
        db.insert("key4", "value4");
        db.remove("key4");
        EXPECT_EQ(db.select("key4"), nullptr);
    }

    UTEST_F(DatabaseTest, SaveToFile) {
        Database db("test_db.txt");
        db.insert("key5", "value5");
        db.saveToFile();

        std::ifstream file("test_db.txt");
        std::string line;
        bool found = false;
        while (std::getline(file, line)) {
            if (line == "key5 value5") {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }

} // namespace DB
