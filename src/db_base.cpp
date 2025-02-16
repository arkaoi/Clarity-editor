#include "db_base.hpp"

namespace DB {
    Database::Database(const std::string& file) : filename(file) {
        loadFromFile();
    }

    void Database::loadFromFile() {
        std::ifstream file(filename);
        if (!file) {
            return;
        }

        std::string key, value;
        while (file >> key >> value) {
            data[key] = value;
        }
        file.close();
    }

    void Database::saveToFile() {
        std::ofstream file(filename, std::ios::trunc);
        for (std::map<std::string, std::string>::const_iterator it = data.begin(); it != data.end(); ++it) {
            file << it->first << " " << it->second << "\n";
        }
        file.close();
    }

    void Database::insert(const std::string& key, const std::string& value) {
        data[key] = value;
    }

    const std::string* Database::select(const std::string& key) {
        auto it = data.find(key);

        if (it != data.end()) {
            return &it->second;
        }

        return nullptr;
    }

    void Database::read(const std::string& key) {
        const std::string* result = select(key);
        if (result) {
            std::cout << key << ": " << *result << "\n";
        }
        else {
            std::cout << key << ": Not Found\n";
        }
    }

    void Database::remove(const std::string& key) {
        data.erase(key);
    }

} // namespace DB
