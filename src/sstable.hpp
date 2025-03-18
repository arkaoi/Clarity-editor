#ifndef SSTABLE_HPP_
#define SSTABLE_HPP_

#include <fstream>
#include <map>
#include <optional>
#include <string>

namespace DB {

class ISSTable {
public:
  virtual void
  write(const std::map<std::string, std::optional<std::string>> &data) = 0;
  virtual bool find(const std::string &key,
                    std::optional<std::string> &value) = 0;
  virtual std::map<std::string, std::optional<std::string>> dump() const = 0;
  virtual ~ISSTable() {}
};

class SSTable : public ISSTable {
private:
  std::string filename;
  std::map<std::string, std::streampos> index;
  int level;
  void loadIndex();

public:
  explicit SSTable(const std::string &file, int lvl = 0);
  void
  write(const std::map<std::string, std::optional<std::string>> &data) override;
  bool find(const std::string &key, std::optional<std::string> &value) override;
  std::map < std::string, std::optional < std::string >>> dump() const override;
  const std::string &getFilename() const { return filename; }
  int getLevel() const { return level; }
};

} // namespace DB

#endif // SSTABLE_HPP_
