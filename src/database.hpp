#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include "db_entry.hpp"
#include "skiplist.hpp"
#include "sstable.hpp"
#include "wal.hpp"

#include <atomic>
#include <memory>
#include <sstream>
#include <userver/engine/async.hpp>
#include <vector>

namespace DB {
class Database {
private:
    SkipListMap<std::string, DBEntry> memtable;
    std::vector<std::shared_ptr<SSTable>> sstables;
    std::unique_ptr<SkipListMap<std::string, DBEntry>> flushBuffer;

    size_t memtableLimit;
    size_t sstableLimit;

    std::string directory;
    DB::WAL wal_;

    mutable userver::engine::Mutex db_mutex;
    std::atomic<bool> mergeInProgress{false};

    void flushMemtable();
    void mergeWorker();
    void recoverFromWAL();
    void loadSSTables();
    std::optional<std::string> selectInternal(const std::string &key);

public:
    Database(const std::string &directory, size_t memLimit, size_t sstLimit);
    ~Database();

    void insert(const std::string &key, const std::string &value);
    bool remove(const std::string &key);
    std::optional<std::string> select(const std::string &key);
    void flush();
    void merge();
};

}  // namespace DB

#endif  // DATABASE_HPP_