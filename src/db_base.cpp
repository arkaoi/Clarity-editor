#include "database.hpp"

namespace DB {
Database::Database(const std::string &file) : sstable(file) {
    std::ifstream in(file);
    if (in) {
        std::string key, value;
        while (in >> key >> value) {
            data[key] = value;
        }
    }
}

void Database::saveToFile() {
    sstable.write(data);
}

void Database::insert(const std::string &key, const std::string &value) {
    data[key] = value;
}

const std::string *Database::select(const std::string &key) {
    auto result = data.find(key);
    if (result != data.end()) {
        return &result->second;
    }

    return nullptr;
}

bool Database::remove(const std::string& key) {
    if (data.find(key) != data.end()) {
        data.erase(key) > 0;
    }
}

}  // namespace DB
