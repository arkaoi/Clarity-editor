#include <gtest/gtest.h>
#include <filesystem>
#include "database.hpp"
#include "userver/engine/task/task_context.hpp"
#include "userver/engine/impl/task_processor.hpp"

class DatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        db = std::make_unique<DB::Database>("test_db", 2, 2);
    }

    void TearDown() override {
        std::filesystem::remove_all("test_db");
    }

    std::unique_ptr<DB::Database> db;
};

TEST_F(DatabaseTest, InsertSelectTest) {
    userver::engine::RunCoro([&]() {
        db->insert("key1", "value1");
        db->insert("key2", "value2");

        EXPECT_EQ(db->select("key1"), std::optional<std::string>("value1"));
        EXPECT_EQ(db->select("key2"), std::optional<std::string>("value2"));
    });
}

TEST_F(DatabaseTest, RemoveTest) {
    userver::engine::RunCoro([&]() {
        db->insert("key1", "value1");
        EXPECT_TRUE(db->remove("key1"));
        EXPECT_FALSE(db->remove("key1"));
        EXPECT_EQ(db->select("key1"), std::nullopt);
    });
}

TEST_F(DatabaseTest, FlushTest) {
    userver::engine::RunCoro([&]() {
        db->insert("key1", "value1");
        db->insert("key2", "value2");
        db->flush();
        EXPECT_EQ(db->select("key1"), std::optional<std::string>("value1"));
    });
}

TEST_F(DatabaseTest, MergeTest) {
    userver::engine::RunCoro([&]() {
        db->insert("key1", "value1");
        db->insert("key2", "value2");
        db->flush();
        
        db->insert("key3", "value3");
        db->insert("key4", "value4");
        db->flush();

        db->merge();

        EXPECT_EQ(db->select("key1"), std::optional<std::string>("value1"));
        EXPECT_EQ(db->select("key4"), std::optional<std::string>("value4"));
    });
}
