#ifndef SSTABLE_HPP_
#define SSTABLE_HPP_

#include "../bloom/bloom.hpp"
#include "../base/db_entry.hpp"
#include "../skiplist/skiplist.hpp"
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <string>

namespace DB {
class ISSTable {
public:
    virtual void write(SkipListMap<std::string, DBEntry> &data) = 0;
    virtual bool find(const std::string &key, DBEntry &entry) const = 0;
    virtual std::map<std::string, DBEntry> dump() const = 0;

    virtual ~ISSTable() {}
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
    bool find(const std::string &key, DBEntry &entry) const override;
    std::map<std::string, DBEntry> dump() const override;

    const std::map<std::string, std::streampos> &GetIndex() const {
        return index;
    }

    const std::string &getFilename() const { return filename; }
};

} // namespace DB

#endif // SSTABLE_HPP_
