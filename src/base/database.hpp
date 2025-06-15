#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <userver/engine/async.hpp>
#include <vector>
#include "../skiplist/skiplist.hpp"
#include "../sstable/sstable.hpp"
#include "../wal/wal.hpp"
#include "db_entry.hpp"

namespace DB {

class Database {
private:
    SkipListMap<std::string, DBEntry> memtable;
    std::vector<std::shared_ptr<SSTable>> sstables;
    std::unique_ptr<SkipListMap<std::string, DBEntry>> flushBuffer;
    size_t memtableLimit;
    size_t sstableLimit;
    std::string directory;
    WAL wal_;
    mutable userver::engine::Mutex db_mutex;
    std::atomic<bool> mergeInProgress{false};

    void flushMemtable();
    void mergeWorker();
    void loadSSTables();
    std::optional<std::vector<uint8_t>> selectInternal(const std::string &key);

public:
    Database(const std::string &directory, size_t memLimit, size_t sstLimit);
    ~Database();

    void insert(const std::string &key, const std::vector<uint8_t> &value);
    bool remove(const std::string &key);
    std::optional<std::vector<uint8_t>> select(const std::string &key);
    void flush();
    void merge();
    void SnapshotCsv(const std::string &csv_path) const;
    void recoverFromWAL();
};

}  // namespace DB

#endif  // DATABASE_HPP_