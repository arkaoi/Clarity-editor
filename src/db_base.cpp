#include <iostream>
#include "database.hpp"
#include <filesystem>

namespace DB {

Database::Database(const std::string& dir, size_t memLimit, size_t sstLimit)
    : memtableLimit(memLimit), sstableLimit(sstLimit), directory(dir) {}

void Database::flushMemtable() {
  static int sstableCounter = 0;
  std::filesystem::create_directories(directory);
    std::map<std::string, DBEntry> toFlush = memtable;

    for (auto it = memtable.begin(); it != memtable.end();) {
        if (!it->second.tombstone)
            it = memtable.erase(it);
        else
            ++it;
    }

    std::ostringstream oss;
  oss << directory << "/sstable_" << sstableCounter++ << ".dat";
  std::string filename = oss.str();

  SSTable sst(filename);
  sst.write(toFlush);
  sstables.push_back(sst);

  memtable.clear();

  if (sstables.size() > sstableLimit) {
    mergeSSTables();
  }
}

void Database::mergeSSTables() {
  std::map<std::string, DBEntry> merged;
  for (const auto& sst : sstables) {
    auto records = sst.dump();
    for (const auto& p : records) {
      merged[p.first] = p.second;
    }
  }
  for (auto it = merged.begin(); it != merged.end();) {
    if (it->second.tombstone) {
      it = merged.erase(it);
    } else {
      ++it;
    }
  }

  static int mergeCounter = 1000;
  std::ostringstream oss;
  oss << directory << "/sstable_" << mergeCounter++ << ".dat";
  std::string filename = oss.str();

  SSTable mergedSST(filename);
  mergedSST.write(merged);

  sstables.clear();
  sstables.push_back(mergedSST);
}

void Database::insert(const std::string& key, const std::string& value) {
  memtable[key] = {value, false};
  if (memtable.size() >= memtableLimit) {
    flushMemtable();
  }
}

bool Database::remove(const std::string& key) {
  auto existing = select(key);
  if (!existing.has_value()) {
    return false;
  }
  memtable[key] = {"", true};
  if (memtable.size() >= memtableLimit) {
    flushMemtable();
  }
  return true;
}

std::optional<std::string> Database::select(const std::string& key) {
  auto it = memtable.find(key);
  if (it != memtable.end()) {
    if (it->second.tombstone) return std::nullopt;
    return it->second.value;
  }
  for (auto sst_it = sstables.rbegin(); sst_it != sstables.rend(); ++sst_it) {
    DBEntry entry;
    if (sst_it->find(key, entry)) {
      if (entry.tombstone) return std::nullopt;
      return entry.value;
    }
  }
  return std::nullopt;
}

void Database::flush() {
  if (!memtable.empty()) {
    flushMemtable();
  }
}

void Database::merge() { mergeSSTables(); }

}  // namespace DB
