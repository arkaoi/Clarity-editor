#include <filesystem>
#include <iostream>
#include "database.hpp"

namespace DB {

Database::Database(const std::string &dir, size_t memLimit, size_t sstLimit)
    : memtableLimit(memLimit), sstableLimit(sstLimit), directory(dir) {
}

void Database::flushMemtable() {
    static int sstableCounter = 0;
    std::filesystem::create_directories(directory);
    std::map<std::string, std::optional<std::string>> toFlush = memtable;

    std::ostringstream oss;
    oss << directory << "/sstable_" << sstableCounter++ << ".dat";
    std::string filename = oss.str();

    SSTable sst(filename, 0);
    sst.write(toFlush);
    sstables.push_back(sst);

    memtable.clear();

    if (sstables.size() > sstableLimit) {
        mergeSSTables();
    }
}

void Database::compactSSTables() {
    if (sstables.size() < 2) {
        return;
    }

    int minLevel = INT32_MAX;
    for (const auto &sst : sstables) {
        if (sst.getLevel() < minLevel) {
            minLevel = sst.getLevel();
        }
    }

    std::vector<size_t> indices;
    for (size_t i = 0; i < sstables.size(); ++i) {
        if (sstables[i].getLevel() == minLevel) {
            indices.push_back(i);
            if (indices.size() == 2) {
                break;
            }
        }
    }

    if (indices.size() < 2) {
        return;
    }

    size_t idx1 = indices[0];
    size_t idx2 = indices[1];

    auto records1 = sstables[idx1].dump();
    auto records2 = sstables[idx2].dump();

    std::map<std::string, std::optional<std::string>> mergedData = records1;

    for (const auto &p : records2) {
        mergedData[p.first] = p.second;
    }

    for (auto it = mergedData.begin(); it != mergedData.end();) {
        if (!it->second.has_value()) {
            it = mergedData.erase(it);
        } else {
            ++it;
        }
    }

    static int mergeCounter = 1000;
    std::ostringstream oss;
    oss << directory << "/sstable_" << mergeCounter++ << ".dat";
    std::string newFilename = oss.str();

    SSTable newSST(newFilename, minLevel + 1);
    newSST.write(mergedData);

    std::filesystem::remove(sstables[idx1].getFilename());
    std::filesystem::remove(sstables[idx2].getFilename());

    sstables.erase(sstables.begin() + idx2);
    sstables.erase(sstables.begin() + idx1);

    sstables.push_back(newSST);
}

void Database::mergeSSTables() {
    compactSSTables();
}

void Database::insert(const std::string &key, const std::string &value) {
    memtable[key] = value;
    if (memtable.size() >= memtableLimit) {
        flushMemtable();
    }
}

bool Database::remove(const std::string &key) {
    auto existing = select(key);

    if (!existing.has_value()) {
        return false;
    }

    memtable[key] = std::nullopt;

    if (memtable.size() >= memtableLimit) {
        flushMemtable();
    }

    return true;
}

std::optional<std::string> Database::select(const std::string &key) {
    auto it = memtable.find(key);

    if (it != memtable.end()) {
        return it->second;
    }

    for (auto sst_it = sstables.rbegin(); sst_it != sstables.rend(); ++sst_it) {
        std::optional<std::string> value;

        if (sst_it->find(key, value)) {
            return value;
        }
    }

    return std::nullopt;
}

void Database::flush() {
    if (!memtable.empty()) {
        flushMemtable();
    }
}

void Database::merge() {
    mergeSSTables();
}

}  // namespace DB
