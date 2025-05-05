#ifndef SSTABLE_HPP_
#define SSTABLE_HPP_

#include "bloom.hpp"
#include "db_entry.hpp"
#include "skiplist.hpp"

#include <map>
#include <mutex>
#include <optional>

namespace DB {
class ISSTable {
public:
    virtual void write(SkipListMap<std::string, DBEntry> &data) = 0;
    virtual bool find(const std::string &key, DBEntry &entry) = 0;
    virtual std::map<std::string, DBEntry> dump() const = 0;

    virtual ~ISSTable() {
    }
};

class SSTable : public ISSTable {
private:
    std::string filename;
    mutable std::mutex indexMutex;
    std::map<std::string, std::streampos> index;
    BloomFilter bf_;
    void loadIndex();

public:
    explicit SSTable(const std::string &file);
    void write(SkipListMap<std::string, DBEntry> &data) override;
    bool find(const std::string &key, DBEntry &entry) override;
    std::map<std::string, DBEntry> dump() const override;

    const std::string &getFilename() const {
        return filename;
    }
};

}  // namespace DB

#endif  // SSTABLE_HPP_