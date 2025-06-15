#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include "database.hpp"

namespace {
static std::atomic<size_t> sstableCounter{0};
}

namespace DB {

Database::Database(const std::string &dir, size_t memLimit, size_t sstLimit)
    : memtable(),
      sstables(),
      flushBuffer(nullptr),
      memtableLimit(memLimit),
      sstableLimit(sstLimit),
      directory(dir),
      wal_(directory + "/wal.log"),
      db_mutex(),
      mergeInProgress(false) {
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
                     const std::string &key, const std::vector<uint8_t> &blob,
                     bool tombstone
                 ) {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        memtable.insert(key, {blob, tombstone});
    });
}

void Database::loadSSTables() {
    namespace fs = std::filesystem;
    if (!fs::exists(directory))
        return;
    size_t maxId = 0;
    for (auto &entry : fs::directory_iterator(directory)) {
        auto path = entry.path().string();
        if (entry.is_regular_file() && entry.path().extension() == ".dat" &&
            path.find("sstable_") != std::string::npos) {
            sstables.emplace_back(std::make_shared<SSTable>(path));
            auto fn = entry.path().filename().string();
            auto p1 = fn.find("sstable_");
            auto p2 = fn.rfind(".dat");
            if (p1 != std::string::npos && p2 != std::string::npos &&
                p2 > p1 + 8) {
                try {
                    size_t id = std::stoull(fn.substr(p1 + 8, p2 - (p1 + 8)));
                    maxId = std::max(maxId, id);
                } catch (...) {
                }
            }
        }
    }
    sstableCounter.store(maxId + 1);
}

void Database::flushMemtable() {
    auto toFlush = std::make_unique<SkipListMap<std::string, DBEntry>>();
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        for (auto it = memtable.begin(); it != memtable.end(); ++it) {
            auto kv = *it;
            toFlush->insert(kv.first, kv.second);
        }
        memtable.clear();
        flushBuffer = std::move(toFlush);
    }
    std::filesystem::create_directories(directory);
    size_t id = sstableCounter++;
    auto path = directory + "/sstable_" + std::to_string(id) + ".dat";
    {
        SSTable writer(path);
        writer.write(*flushBuffer);
    }
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        sstables.emplace_back(std::make_shared<SSTable>(path));
        flushBuffer.reset();
        wal_.clear();
        if (sstables.size() > sstableLimit && !mergeInProgress.exchange(true)) {
            userver::engine::CriticalAsyncNoSpan([this] { mergeWorker(); }
            ).Detach();
        }
    }
}

void Database::mergeWorker() {
    userver::engine::current_task::CancellationPoint();
    std::vector<std::shared_ptr<SSTable>> old_list;
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        old_list = sstables;
    }
    SkipListMap<std::string, DBEntry> merged;
    std::vector<std::string> oldFiles;
    for (const auto &sst : old_list) {
        userver::engine::current_task::CancellationPoint();
        auto dumpMap = sst->dump();
        for (const auto &p : dumpMap)
            merged.insert(p.first, p.second);
        oldFiles.push_back(sst->getFilename());
    }
    std::vector<std::string> keysToRemove;
    for (auto it = merged.begin(); it != merged.end(); ++it) {
        auto kv = *it;
        if (kv.second.tombstone) {
            keysToRemove.push_back(kv.first);
        }
    }

    for (const auto &k : keysToRemove) {
        merged.erase(k);
    }
    userver::engine::current_task::CancellationPoint();
    std::ostringstream oss;
    oss << directory << "/sstable_" << std::time(nullptr) << ".dat";
    SSTable newSST(oss.str());
    newSST.write(merged);
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        for (const auto &fn : oldFiles)
            std::filesystem::remove(fn);
        std::vector<std::shared_ptr<SSTable>> survivors;
        for (const auto &sst : sstables) {
            bool keep = true;
            for (auto &of : oldFiles)
                if (sst->getFilename() == of) {
                    keep = false;
                    break;
                }
            if (keep)
                survivors.emplace_back(sst);
        }
        survivors.emplace_back(std::make_shared<SSTable>(newSST.getFilename()));
        sstables = std::move(survivors);
        mergeInProgress.store(false);
    }
}

std::optional<std::vector<uint8_t>> Database::selectInternal(
    const std::string &key
) {
    {
        auto *vPtr = memtable.find(key);
        if (vPtr) {
            if (vPtr->tombstone)
                return std::nullopt;
            return vPtr->value;
        }
        if (flushBuffer) {
            auto *fbPtr = flushBuffer->find(key);
            if (fbPtr) {
                if (fbPtr->tombstone)
                    return std::nullopt;
                return fbPtr->value;
            }
        }
    }
    for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
        DBEntry e;
        if ((*it)->find(key, e)) {
            if (e.tombstone)
                return std::nullopt;
            return e.value;
        }
    }
    return std::nullopt;
}

void Database::insert(
    const std::string &key,
    const std::vector<uint8_t> &value
) {
    bool need = false;
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        wal_.logInsert(key, value);
        memtable.insert(key, {value, false});
        need = (memtable.size() >= memtableLimit);
    }
    if (need)
        flushMemtable();
}

bool Database::remove(const std::string &key) {
    bool existed = false, need = false;
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        auto prev = selectInternal(key);
        if (!prev.has_value())
            return false;
        wal_.logRemove(key);
        memtable.insert(key, {{}, true});
        existed = true;
        need = (memtable.size() >= memtableLimit);
    }
    if (need)
        flushMemtable();
    return existed;
}

std::optional<std::vector<uint8_t>> Database::select(const std::string &key) {
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
        userver::engine::CriticalAsyncNoSpan([this] { mergeWorker(); }
        ).Detach();
    }
}

void Database::SnapshotCsv(const std::string &csv_path) const {
    std::vector<std::shared_ptr<SSTable>> sst_copy;
    SkipListMap<std::string, DBEntry> mem_copy;
    std::unique_ptr<SkipListMap<std::string, DBEntry>> flush_copy;
    {
        std::lock_guard<userver::engine::Mutex> lock(db_mutex);
        for (auto it = memtable.begin(); it != memtable.end(); ++it) {
            auto p = *it;
            mem_copy.insert(p.first, p.second);
        }
        if (flushBuffer) {
            flush_copy = std::make_unique<SkipListMap<std::string, DBEntry>>();
            for (auto it = flushBuffer->begin(); it != flushBuffer->end();
                 ++it) {
                auto p = *it;
                flush_copy->insert(p.first, p.second);
            }
        }
        sst_copy = sstables;
    }
    std::map<std::string, DBEntry> full_map;
    for (auto it = mem_copy.begin(); it != mem_copy.end(); ++it) {
        auto p = *it;
        full_map[p.first] = p.second;
    }
    if (flush_copy) {
        for (auto it = flush_copy->begin(); it != flush_copy->end(); ++it) {
            auto p = *it;
            full_map[p.first] = p.second;
        }
    }
    for (auto &sst : sst_copy) {
        auto dm = sst->dump();
        for (auto &p : dm)
            full_map[p.first] = p.second;
    }
    for (auto it = full_map.begin(); it != full_map.end();) {
        if (it->second.tombstone)
            it = full_map.erase(it);
        else
            ++it;
    }
    std::ofstream out(csv_path);
    if (!out)
        throw std::runtime_error("Cannot open CSV");
    out << "key,value\n";
    for (auto &pr : full_map) {
        const auto &key = pr.first;
        const auto &e = pr.second;
        std::string v(e.value.begin(), e.value.end());
        out << key << ",\"";
        for (char c : v) {
            if (c == '"')
                out << "\"\"";
            else
                out << c;
        }
        out << "\"\n";
    }
}

}  // namespace DB