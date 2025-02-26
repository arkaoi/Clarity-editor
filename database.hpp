#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace DB {

class SSTable {
private:
    std::string filename;

    static bool compare_keys(
        const std::pair<std::string, std::string> &a,
        const std::pair<std::string, std::string> &b
    );

public:
    SSTable(const std::string &file);
    void write(const std::map<std::string, std::string> &data);
    const std::string read(const std::string &key);
};

class Database {
private:
    std::map<std::string, std::string> data;
    SSTable sstable;

public:
    explicit Database(const std::string &file);
    void insert(const std::string &key, const std::string &value);
    const std::string *select(const std::string &key);
    void remove(const std::string &key);
    void saveToFile();
};

}  // namespace DB

#endif  // DATABASE_HPP_
