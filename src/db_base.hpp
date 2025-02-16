#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include <iostream>
#include <fstream>
#include <map>
#include <string>

namespace DB {

    class Database {
    private:
        std::map<std::string, std::string> data;
        std::string filename;

        void loadFromFile();

    public:
        Database(const std::string& file);
        void insert(const std::string& key, const std::string& value);
        const std::string* select(const std::string& key);
        void read(const std::string& key);
        void remove(const std::string& key);
        void saveToFile();
    };

} // namespace DB

#endif // DATABASE_HPP_
