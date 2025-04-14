#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <unistd.h>

#include "wal.hpp"

namespace DB {

using boost_file_sink =
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink>;
using boost_file_source =
    boost::iostreams::stream<boost::iostreams::file_descriptor_source>;

WAL::WAL(const std::string &filename) : filename_(filename) {
    fd_out_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_out_ < 0) {
        std::cerr << "Failed to open WAL file: " << filename_ << "\n";
    }
}

WAL::~WAL() {
    if (fd_out_ >= 0) {
        ::close(fd_out_);
    }
}

void WAL::logInsert(const std::string &key, const std::string &value) {
    std::lock_guard<userver::engine::Mutex> lock(walMutex_);
    if (fd_out_ < 0) {
        return;
    }

    boost_file_sink out(fd_out_, boost::iostreams::never_close_handle);
    out << "INSERT " << key << " " << value << "\n";
    out.flush();
}

void WAL::logRemove(const std::string &key) {
    std::lock_guard<userver::engine::Mutex> lock(walMutex_);
    if (fd_out_ < 0) {
        return;
    }

    boost_file_sink out(fd_out_, boost::iostreams::never_close_handle);
    out << "REMOVE " << key << "\n";
    out.flush();
}

void WAL::recover(
    std::function<void(const std::string &, const std::string &, bool)>
        applyOperation
) {
    int fd = ::open(filename_.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "No WAL file found for recovery.\n";
        return;
    }
    boost_file_source in(fd, boost::iostreams::close_handle);

    std::string line;
    while (std::getline(in, line)) {
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
    if (fd_out_ >= 0) {
        ::close(fd_out_);
    }

    int fd_clear =
        ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_clear >= 0) {
        ::close(fd_clear);
    }
    fd_out_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
}

}  // namespace DB
