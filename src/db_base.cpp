#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include "database.hpp"
#include "db_config.hpp"
#include "sstable.hpp"

namespace DB {

Database::Database(const std::string& dir, size_t memLimit, size_t sstLimit)
    : memtableLimit(memLimit),
      sstableLimit(sstLimit),
      directory(dir),
      wal_(std::string(DBConfig::kDirectory) + "/wal.log") {
  recoverFromWAL();
}

Database::~Database() { flush(); }

void Database::recoverFromWAL() {
  wal_.recover([this](const std::string& key, const std::string& value,
                      bool tombstone) { memtable[key] = {value, tombstone}; });
}

void Database::flushMemtable() {
  static int sstableCounter = 0;
  std::filesystem::create_directories(directory);
  std::map<std::string, DBEntry> toFlush = memtable;

  std::ostringstream oss;
  oss << directory << "/sstable_" << sstableCounter++ << ".dat";
  std::string filename = oss.str();

  SSTable sst(filename);
  sst.write(toFlush);
  sstables.push_back(sst);

  memtable.clear();
  wal_.clear();

  if (sstables.size() > sstableLimit) {
    mergeSSTables();
  }
}

void Database::mergeSSTables() {
  std::map<std::string, DBEntry> merged;
  std::vector<std::string> oldFiles;
  for (const auto& sst : sstables) {
    auto records = sst.dump();
    for (const auto& p : records) {
      merged[p.first] = p.second;
    }
    oldFiles.push_back(sst.getFilename());
  }
  for (auto it = merged.begin(); it != merged.end();) {
    if (it->second.tombstone) {
      it = merged.erase(it);
    } else {
      ++it;
    }
  }

  auto now = std::chrono::system_clock::now();
  auto now_c = std::chrono::system_clock::to_time_t(now);
  std::ostringstream oss;
  oss << directory << "/sstable_" << now_c << ".dat";
  std::string filename = oss.str();

  SSTable mergedSST(filename);
  mergedSST.write(merged);

  for (const auto& file : oldFiles) {
    std::error_code ec;
    std::filesystem::remove(file, ec);
    if (ec) {
      std::cerr << "Error removing file " << file << ": " << ec.message()
                << "\n";
    }
  }

  sstables.clear();
  sstables.push_back(mergedSST);
}

std::optional<std::string> Database::selectInternal(const std::string& key) {
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

void Database::insert(const std::string& key, const std::string& value) {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  wal_.logInsert(key, value);
  memtable[key] = {value, false};
  if (memtable.size() >= memtableLimit) {
    flushMemtable();
  }
}

bool Database::remove(const std::string& key) {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  auto existing = selectInternal(key);
  if (!existing.has_value()) {
    return false;
  }
  wal_.logRemove(key);
  memtable[key] = {"", true};
  if (memtable.size() >= memtableLimit) {
    flushMemtable();
  }
  return true;
}

std::optional<std::string> Database::select(const std::string& key) {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  return selectInternal(key);
}

void Database::flush() {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  if (!memtable.empty()) {
    flushMemtable();
  }
}

void Database::merge() {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  mergeSSTables();
}

}  // namespace DB
