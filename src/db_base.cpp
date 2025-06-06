#include <chrono>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <sstream>
#include "database.hpp"

namespace {
static std::atomic<size_t> sstableCounter{0};
}

namespace DB {

Database::Database(const std::string &dir, size_t memLimit, size_t sstLimit)
    : memtableLimit(memLimit),
      sstableLimit(sstLimit),
      directory(dir),
      wal_(directory + "/wal.log") {
    std::filesystem::create_directories(directory);
    recoverFromWAL();
    loadSSTables();
}

Database::~Database() {
    std::lock_guard<userver::engine::Mutex> lock(db_mutex);
    if (!memtable.empty()) {
        flushMemtable();
    }
}

void Database::recoverFromWAL() {
    wal_.recover([this](
                     const std::string &key,
                     const std::vector<uint8_t> &valueBlob, bool tombstone
                 ) {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        memtable.insert(key, {valueBlob, tombstone});
    });
}

void Database::loadSSTables() {
    namespace fs = std::filesystem;
    if (!fs::exists(directory)) {
        return;
    }
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
            if (p1 != std::string::npos && p2 != std::string::npos &&
                p2 > p1 + 8) {
                try {
                    size_t id =
                        std::stoull(filename.substr(p1 + 8, p2 - (p1 + 8)));
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
    auto toFlush = std::make_unique<SkipListMap<std::string, DBEntry>>();
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);

        for (auto it = memtable.begin(); it != memtable.end(); ++it) {
            const auto &kv = *it;
            toFlush->insert(kv.first, kv.second);
        }

        memtable.clear();
        wal_.clear();

        flushBuffer = std::move(toFlush);
    }
    std::filesystem::create_directories(directory);

    size_t id = sstableCounter++;
    const std::string path =
        directory + "/sstable_" + std::to_string(id) + ".dat";

    {
        SSTable writer(path);
        writer.write(*flushBuffer);
    }

    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);

        sstables.push_back(std::make_shared<SSTable>(path));
        flushBuffer.reset();
        if (sstables.size() > sstableLimit && !mergeInProgress.exchange(true)) {
            userver::engine::CriticalAsyncNoSpan([this] { mergeWorker(); }
            ).Detach();
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

    SkipListMap<std::string, DBEntry> merged;
    std::vector<std::string> oldFiles;
    for (auto &sst : batch) {
        userver::engine::current_task::CancellationPoint();
        auto dump = sst->dump();
        for (auto &p : dump) {
            merged.insert(p.first, p.second);
        }
        oldFiles.push_back(sst->getFilename());
    }

    std::vector<std::string> keysToRemove;
    for (auto it = merged.begin(); it != merged.end(); ++it) {
        const auto &kv = *it;
        if (kv.second.tombstone) {
            keysToRemove.push_back(kv.first);
        }
    }

    for (const auto &key : keysToRemove) {
        merged.erase(key);
    }

    userver::engine::current_task::CancellationPoint();
    std::ostringstream oss;
    oss << directory << "/sstable_" << std::time(nullptr) << ".dat";
    SSTable newSST(oss.str());
    newSST.write(merged);
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        for (auto &f : oldFiles) {
            std::filesystem::remove(f);
        }
        sstables.clear();
        sstables.push_back(std::make_shared<SSTable>(newSST.getFilename()));
        mergeInProgress.store(false);
    }
}

std::optional<std::vector<uint8_t>> Database::selectInternal(
    const std::string &key
) {
    {
        DBEntry *valuePtr = memtable.find(key);
        if (valuePtr != nullptr) {
            if (valuePtr->tombstone) {
                return std::nullopt;
            }
            return valuePtr->value;
        }
        if (flushBuffer) {
            DBEntry *fbValuePtr = flushBuffer->find(key);
            if (fbValuePtr != nullptr) {
                if (fbValuePtr->tombstone) {
                    return std::nullopt;
                }
                return fbValuePtr->value;
            }
        }
    }

    for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
        DBEntry entry;
        if ((*it)->find(key, entry)) {
            if (entry.tombstone) {
                return std::nullopt;
            }
            return entry.value;
        }
    }

    return std::nullopt;
}

void Database::insert(
    const std::string &key,
    const std::vector<uint8_t> &value
) {
    bool shouldFlush = false;

    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        wal_.logInsert(key, value);
        memtable.insert(key, {value, false});
        shouldFlush = (memtable.size() >= memtableLimit);
    }

    if (shouldFlush) {
        flushMemtable();
    }
}

bool Database::remove(const std::string &key) {
    bool existed = false;
    bool shouldFlush = false;

    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        auto existing = selectInternal(key);
        if (!existing.has_value()) {
            return false;
        }
        wal_.logRemove(key);
        memtable.insert(key, {{}, true});
        existed = true;
        shouldFlush = (memtable.size() >= memtableLimit);
    }

    if (shouldFlush) {
        flushMemtable();
    }

    return existed;
}

std::optional<std::vector<uint8_t>> Database::select(const std::string &key) {
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

    if (!mergeInProgress.exchange(true)) {
        userver::engine::CriticalAsyncNoSpan([this] { mergeWorker(); }
        ).Detach();
    }
}

}  // namespace DB