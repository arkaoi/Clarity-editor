#include "wal.hpp"

#include <iostream>
#include <sstream>

namespace DB {

WAL::WAL(const std::string& filename) : filename_(filename) {
  ofs_.open(filename_, std::ios::app);
  if (!ofs_) {
    std::cerr << "Failed to open WAL file: " << filename_ << "\n";
  }
}

WAL::~WAL() {
  if (ofs_.is_open()) {
    ofs_.close();
  }
}

void WAL::logInsert(const std::string& key, const std::string& value) {
  std::lock_guard<userver::engine::Mutex> lock(walMutex_);
  ofs_ << "INSERT " << key << " " << value << "\n";
  ofs_.flush();
}

void WAL::logRemove(const std::string& key) {
  std::lock_guard<userver::engine::Mutex> lock(walMutex_);
  ofs_ << "REMOVE " << key << "\n";
  ofs_.flush();
}

void WAL::recover(
    std::function<void(const std::string&, const std::string&, bool)>
        applyOperation) {
  std::ifstream ifs(filename_);
  if (!ifs) {
    std::cerr << "No WAL file found for recovery.\n";
    return;
  }
  std::string line;
  while (std::getline(ifs, line)) {
    std::istringstream iss(line);
    std::string op;
    iss >> op;
    if (op == "INSERT") {
      std::string key, value;
      iss >> key >> value;
      applyOperation(key, value, false);
    } else if (op == "REMOVE") {
      std::string key;
      iss >> key;
      applyOperation(key, "", true);
    }
  }
}

void WAL::clear() {
  std::lock_guard<userver::engine::Mutex> lock(walMutex_);
  ofs_.close();
  std::ofstream ofs_clear(filename_, std::ios::trunc);
  ofs_clear.close();
  ofs_.open(filename_, std::ios::app);
}

}  // namespace DB
