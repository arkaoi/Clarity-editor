#include "database.hpp"
#include "sstable.hpp"
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <userver/engine/async.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/task/cancel.hpp>

namespace {
static std::atomic<size_t> sstableCounter{0};
}

namespace DB {

Database::Database(const std::string &dir, size_t memLimit, size_t sstLimit)
    : memtableLimit(memLimit), sstableLimit(sstLimit), directory(dir),
      wal_(directory + "/wal.log") {
  std::filesystem::create_directories(directory);
  recoverFromWAL();
  loadSSTables();
}

Database::~Database() {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  if (!memtable.empty())
    flushMemtable();
}

void Database::recoverFromWAL() {
  wal_.recover(
      [this](const std::string &key, const std::string &value, bool tombstone) {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        memtable[key] = {value, tombstone};
      });
}

void Database::loadSSTables() {
  namespace fs = std::filesystem;
  if (!fs::exists(directory))
    return;

  size_t maxId = 0;
  bool hasFiles = false;
  for (auto &entry : fs::directory_iterator(directory)) {
    auto path = entry.path().string();
    if (entry.is_regular_file() && entry.path().extension() == ".dat" &&
        path.find("sstable_") != std::string::npos) {
      sstables.push_back(std::make_shared<SSTable>(path));
      auto filename = entry.path().filename().string();
      auto p1 = filename.find("sstable_");
      auto p2 = filename.rfind(".dat");
      if (p1 != std::string::npos && p2 != std::string::npos && p2 > p1 + 8) {
        try {
          size_t id = std::stoull(filename.substr(p1 + 8, p2 - (p1 + 8)));
          maxId = std::max(maxId, id);
          hasFiles = true;
        } catch (...) {
        }
      }
    }
  }
  if (hasFiles) {
    sstableCounter.store(maxId + 1);
  }
}

void Database::flushMemtable() {
  std::map<std::string, DBEntry> toFlush;
  {
    std::lock_guard<userver::engine::Mutex> lock(db_mutex);
    toFlush = std::move(memtable);
    memtable.clear();
    wal_.clear();
    flushBuffer = toFlush;
  }

  std::filesystem::create_directories(directory);
  size_t id = sstableCounter++;
  const std::string path =
      directory + "/sstable_" + std::to_string(id) + ".dat";

  {
    SSTable writer(path);
    writer.write(toFlush);
  }

  {
    std::lock_guard<userver::engine::Mutex> lock(db_mutex);
    sstables.push_back(std::make_shared<SSTable>(path));
    flushBuffer.reset();
    if (sstables.size() > sstableLimit && !mergeInProgress.exchange(true)) {
      userver::engine::CriticalAsyncNoSpan([this] { mergeWorker(); }).Detach();
    }
  }
}

void Database::mergeWorker() {
  userver::engine::current_task::CancellationPoint();

  std::vector<std::shared_ptr<SSTable>> batch;
  {
    std::lock_guard<userver::engine::Mutex> lock(db_mutex);
    batch = sstables;
  }

  std::map<std::string, DBEntry> merged;
  std::vector<std::string> oldFiles;

  for (auto &sst : batch) {
    userver::engine::current_task::CancellationPoint();
    for (auto &p : sst->dump()) {
      merged[p.first] = p.second;
    }
    oldFiles.push_back(sst->getFilename());
  }

  for (auto it = merged.begin(); it != merged.end();) {
    if (it->second.tombstone)
      it = merged.erase(it);
    else
      ++it;
  }

  userver::engine::current_task::CancellationPoint();

  std::ostringstream oss;
  oss << directory << "/sstable_" << std::time(nullptr) << ".dat";
  SSTable newSST(oss.str());
  newSST.write(merged);

  {
    std::lock_guard<userver::engine::Mutex> lock(db_mutex);
    for (auto &f : oldFiles)
      std::filesystem::remove(f);
    sstables.clear();
    sstables.push_back(std::make_shared<SSTable>(newSST.getFilename()));
    mergeInProgress.store(false);
  }
}

std::optional<std::string> Database::selectInternal(const std::string &key) {
  {

    auto it = memtable.find(key);
    if (it != memtable.end()) {
      if (it->second.tombstone)
        return std::nullopt;
      return it->second.value;
    }

    if (flushBuffer) {
      auto fbIt = flushBuffer->find(key);
      if (fbIt != flushBuffer->end()) {
        if (fbIt->second.tombstone)
          return std::nullopt;
        return fbIt->second.value;
      }
    }
  }

  for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
    DBEntry entry;
    if ((*it)->find(key, entry)) {
      if (entry.tombstone)
        return std::nullopt;
      return entry.value;
    }
  }
  return std::nullopt;
}

void Database::insert(const std::string &key, const std::string &value) {
  bool shouldFlush = false;
  {
    std::lock_guard<userver::engine::Mutex> lock(db_mutex);
    wal_.logInsert(key, value);
    memtable[key] = {value, false};
    shouldFlush = (memtable.size() >= memtableLimit);
  }
  if (shouldFlush)
    flushMemtable();
}

bool Database::remove(const std::string &key) {
  bool existed = false;
  bool shouldFlush = false;
  {
    std::lock_guard<userver::engine::Mutex> lock(db_mutex);
    auto existing = selectInternal(key);
    if (!existing.has_value())
      return false;
    wal_.logRemove(key);
    memtable[key] = {"", true};
    existed = true;
    shouldFlush = (memtable.size() >= memtableLimit);
  }
  if (shouldFlush)
    flushMemtable();
  return existed;
}

std::optional<std::string> Database::select(const std::string &key) {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  return selectInternal(key);
}

void Database::flush() {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  if (!memtable.empty())
    flushMemtable();
}

void Database::merge() {
  std::lock_guard<userver::engine::Mutex> lock(db_mutex);
  if (!mergeInProgress.exchange(true)) {
    userver::engine::CriticalAsyncNoSpan([this] { mergeWorker(); }).Detach();
  }
}

} // namespace DB
